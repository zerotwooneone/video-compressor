# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "rich",
# ]
# ///

import argparse
import concurrent.futures
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".m4v", ".webm", ".flv", ".wmv"}
console = Console()

# Thread-safe tracker to prevent multiple workers from over-committing disk space
disk_space_lock = threading.Lock()
reserved_disk_space = 0

# --- Data Models ---

@dataclass
class VideoStats:
    path: Path
    size_bytes: int
    duration_sec: float
    bitrate_bps: int
    codec: str
    est_converted_size_bytes: int

@dataclass
class DirStats:
    path: Path
    file_count: int
    total_size_bytes: int
    total_duration_sec: float
    dominant_codec: str
    est_converted_size_bytes: int
    avg_bitrate: int

    @property
    def est_recovered_bytes(self) -> int:
        return max(0, self.total_size_bytes - self.est_converted_size_bytes)

    @property
    def avg_mb_per_min(self) -> float:
        if self.total_duration_sec == 0: return 0.0
        return (self.total_size_bytes / (1024 * 1024)) / (self.total_duration_sec / 60.0)

# --- Core Logic ---

def estimate_savings(codec: str, size_bytes: int, crf: int, use_nvenc: bool) -> int:
    """Estimates the final file size, factoring in the codec base and the hardware penalty."""
    codec = codec.lower()
    base_reductions = {
        'hevc': 0.0,
        'h264': 0.45,
        'mpeg4': 0.60,
        'mpeg2video': 0.75,
        'prores': 0.90
    }
    
    crf_diff = crf - 28
    crf_multiplier = 2 ** (-crf_diff / 6.0)
    base_reduction = base_reductions.get(codec, 0.30)
    
    if use_nvenc and codec != 'hevc':
        base_reduction *= 0.80  # ~20% penalty for hardware encoding efficiency

    target_ratio = max(0.05, min((1.0 - base_reduction) * crf_multiplier, 1.0))
    return int(size_bytes * target_ratio)

def probe_video(file_path: Path, crf: int, use_nvenc: bool) -> Tuple[Optional[VideoStats], str]:
    """Extracts video metadata concurrently using ffprobe."""
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_format", "-show_streams", str(file_path)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        data = json.loads(result.stdout)
        
        format_info = data.get("format", {})
        video_streams = [s for s in data.get("streams", []) if s.get("codec_type") == "video"]
        
        if not video_streams:
            return None, "No video stream found"
            
        video_stream = video_streams[0]
        size = int(float(format_info.get("size", 0)))
        duration = float(format_info.get("duration", 0))
        
        if size == 0 or duration == 0:
            return None, "Invalid size or duration"

        bitrate = int(format_info.get("bit_rate", 0)) or int((size * 8) / duration)
        codec = video_stream.get("codec_name", "unknown")
        est_size = estimate_savings(codec, size, crf, use_nvenc)

        stats = VideoStats(
            path=file_path, size_bytes=size, duration_sec=duration,
            bitrate_bps=bitrate, codec=codec, est_converted_size_bytes=est_size
        )
        return stats, ""
    except Exception as e:
        return None, f"Probe error: {str(e)}"

def build_ffmpeg_cmd(stats: VideoStats, crf: int, use_nvenc: bool, out_path: Path) -> List[str]:
    """Generates the exact ffmpeg command array based on hardware preferences."""
    if use_nvenc:
        video_args = ["-c:v", "hevc_nvenc", "-cq", str(crf), "-preset", "p6", "-profile:v", "main10", "-pix_fmt", "yuv420p10le"]
    else:
        video_args = ["-c:v", "libx265", "-crf", str(crf), "-preset", "medium", "-pix_fmt", "yuv420p10le"]

    return [
        "ffmpeg", "-y", "-i", str(stats.path),
        "-map", "0"
    ] + video_args + [
        "-c:a", "copy",
        "-c:s", "copy",
        str(out_path)
    ]

def convert_and_verify(stats: VideoStats, crf: int, delete_original: bool, use_nvenc: bool) -> Tuple[bool, Path, str]:
    """The main worker function that safely executes the video conversion."""
    global reserved_disk_space
    out_path = stats.path.with_suffix(f".hevc_crf{crf}.mkv")

    # Guardrail: Prevent identical input/output truncation
    if stats.path.resolve() == out_path.resolve():
        return False, stats.path, "Safety abort: Input and Output paths are identical"

    # Guardrail: Thread-safe disk space check (requires est size + 5GB buffer per worker)
    with disk_space_lock:
        free_space = shutil.disk_usage(out_path.parent).free
        if (free_space - reserved_disk_space) < (stats.est_converted_size_bytes + (5 * 1024**3)):
            return False, stats.path, "Skipped: Insufficient free disk space"
        reserved_disk_space += stats.est_converted_size_bytes

    cmd = build_ffmpeg_cmd(stats, crf, use_nvenc, out_path)
    
    try:
        # Run ffmpeg, suppressing stdout/stderr unless an error occurs
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        
        # Guardrail: Sanity check duration
        verify_stats, error = probe_video(out_path, crf, use_nvenc)
        if not verify_stats or abs(stats.duration_sec - verify_stats.duration_sec) > max(stats.duration_sec * 0.01, 1.0):
            raise ValueError(f"Sanity check failed: Duration mismatch or probe error ({error}).")
            
        if delete_original:
            stats.path.unlink(missing_ok=True)
            
        return True, stats.path, f"Converted successfully ({'NVENC' if use_nvenc else 'CPU'})"

    except Exception as e:
        out_path.unlink(missing_ok=True) # Clean up partial/broken files
        return False, stats.path, str(e)
    finally:
        with disk_space_lock:
            reserved_disk_space -= stats.est_converted_size_bytes

# --- Aggregation & Reporting ---

def format_size(bytes_val: int) -> str:
    return f"{(bytes_val / (1024**3)):.2f} GB"

def render_table(videos: List[VideoStats]):
    dir_map: Dict[Path, List[VideoStats]] = {}
    for v in videos:
        dir_map.setdefault(v.path.parent, []).append(v)

    aggregates = []
    for directory, vids in dir_map.items():
        codecs = [v.codec for v in vids]
        bitrates = [v.bitrate_bps for v in vids]
        aggregates.append(DirStats(
            path=directory, file_count=len(vids),
            total_size_bytes=sum(v.size_bytes for v in vids),
            total_duration_sec=sum(v.duration_sec for v in vids),
            dominant_codec=Counter(codecs).most_common(1)[0][0],
            est_converted_size_bytes=sum(v.est_converted_size_bytes for v in vids),
            avg_bitrate=sum(bitrates) // len(bitrates) if bitrates else 0
        ))

    aggregates.sort(key=lambda x: x.est_recovered_bytes, reverse=True)

    table = Table(title="\nVideo Directory Analysis (Sorted by Recoverable Space)")
    table.add_column("Directory", style="cyan", no_wrap=True)
    table.add_column("Files", justify="right")
    table.add_column("Codec", style="magenta")
    table.add_column("Current Size", justify="right", style="red")
    table.add_column("Est. Recovered", justify="right", style="bold green")

    for stat in aggregates:
        table.add_row(
            str(stat.path), str(stat.file_count), stat.dominant_codec.upper(),
            format_size(stat.total_size_bytes), format_size(stat.est_recovered_bytes)
        )
    console.print(table)
    console.print(f"\n[bold green]Total Estimated Recoverable Space: {format_size(sum(d.est_recovered_bytes for d in aggregates))}[/bold green]\n")

# --- Main CLI ---

def main():
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        console.print("[bold red]Error: 'ffmpeg' and 'ffprobe' are required in PATH.[/bold red]")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Analyze and safely compress video directories.")
    parser.add_argument("target", type=Path, help="Target directory to scan")
    parser.add_argument("--crf", type=int, default=28, choices=range(0, 52), help="Target quality (default: 28)")
    parser.add_argument("--nvenc", action="store_true", help="Use NVIDIA hardware encoding")
    parser.add_argument("--min-savings", type=int, default=10, help="Min % space saved to justify conversion")
    parser.add_argument("--convert", action="store_true", help="Run ffmpeg conversions")
    parser.add_argument("--delete-original", action="store_true", help="Delete original files on success")
    parser.add_argument("--dry-run", action="store_true", help="Print intended ffmpeg commands and exit")
    parser.add_argument("--verbose", action="store_true", help="Print verbose file stats")
    parser.add_argument("--probe-workers", type=int, default=max(1, (os.cpu_count() or 2) - 1))
    parser.add_argument("--convert-workers", type=int, default=1)
    
    args = parser.parse_args()
    if args.delete_original: args.convert = True

    if not args.target.is_dir():
        console.print(f"[bold red]Error: '{args.target}' is not a valid directory.[/bold red]")
        sys.exit(1)

    video_files = [p for p in args.target.rglob("*") if p.suffix.lower() in VIDEO_EXTENSIONS]
    if not video_files:
        console.print("[yellow]No video files found.[/yellow]")
        return

    analyzed_videos = []
    with Progress(SpinnerColumn(), TextColumn("[cyan]Probing videos..."), BarColumn(), TaskProgressColumn()) as progress:
        task = progress.add_task("", total=len(video_files))
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.probe_workers) as executor:
            futures = {executor.submit(probe_video, f, args.crf, args.nvenc): f for f in video_files}
            for future in concurrent.futures.as_completed(futures):
                stats, error = future.result()
                if stats: analyzed_videos.append(stats)
                elif args.verbose: progress.console.print(f"[red]Probe failed ({futures[future].name}): {error}[/red]")
                progress.advance(task)

    if not analyzed_videos: return
    render_table(analyzed_videos)

    # Filter targets
    targets = []
    for v in analyzed_videos:
        if v.codec.lower() == 'hevc': continue
        savings_pct = (v.size_bytes - v.est_converted_size_bytes) / max(1, v.size_bytes)
        if savings_pct >= (args.min_savings / 100.0):
            targets.append(v)

    if not targets:
        console.print("[green]No files meet the conversion criteria.[/green]")
        return

    # Dry Run Execution
    if args.dry_run:
        console.print(f"\n[bold yellow]--- DRY RUN: {len(targets)} Files Targeted ---[/bold yellow]")
        for v in targets:
            cmd = build_ffmpeg_cmd(v, args.crf, args.nvenc, v.path.with_suffix(f".hevc_crf{args.crf}.mkv"))
            console.print(f"[cyan]File:[/cyan] {v.path.name}")
            console.print(f"[dim]{' '.join(cmd)}[/dim]\n")
        return

    # Real Execution
    if args.convert:
        console.print(f"[bold blue]Starting conversion of {len(targets)} files...[/bold blue]")
        with Progress(SpinnerColumn(), TextColumn("[cyan]Converting..."), BarColumn(), TaskProgressColumn()) as progress:
            task = progress.add_task("", total=len(targets))
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.convert_workers) as executor:
                conv_futures = {executor.submit(convert_and_verify, v, args.crf, args.delete_original, args.nvenc): v for v in targets}
                for future in concurrent.futures.as_completed(conv_futures):
                    success, path, msg = future.result()
                    if success and args.verbose:
                        progress.console.print(f"[green]Success: {path.name} ({msg})[/green]")
                    elif not success:
                        progress.console.print(f"[red]Failed: {path.name} -> {msg}[/red]")
                    progress.advance(task)
        console.print("\n[bold green]Batch processing complete.[/bold green]")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[bold red]Interrupt received. Force-killing all child processes...[/bold red]")
        # Pragmatic cross-platform process killer (Optimized for Unix/Ubuntu servers)
        try:
            os.killpg(os.getpgid(0), signal.SIGKILL)
        except AttributeError:
            # Fallback for Windows where killpg is not available
            sys.exit(1)