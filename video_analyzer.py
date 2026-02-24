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
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".m4v", ".webm", ".flv", ".wmv"}
console = Console()

# --- Helpers & Data Models ---

def safe_float(val, default=0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0) -> int:
    try:
        return int(float(val)) if val is not None else default
    except (ValueError, TypeError):
        return default

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
    min_bitrate: int
    max_bitrate: int
    avg_bitrate: int

    @property
    def est_recovered_bytes(self) -> int:
        return max(0, self.total_size_bytes - self.est_converted_size_bytes)

    @property
    def avg_mb_per_min(self) -> float:
        if self.total_duration_sec == 0:
            return 0.0
        return (self.total_size_bytes / (1024 * 1024)) / (self.total_duration_sec / 60.0)

# --- Core Logic ---

def estimate_savings(codec: str, size_bytes: int, crf: int) -> int:
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

    target_ratio = (1.0 - base_reduction) * crf_multiplier
    target_ratio = max(0.05, min(target_ratio, 1.0))

    return int(size_bytes * target_ratio)

def probe_video(file_path: Path, crf: int) -> Tuple[Optional[VideoStats], Optional[str]]:
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
        size = safe_int(format_info.get("size"))
        duration = safe_float(format_info.get("duration"))

        bitrate = safe_int(format_info.get("bit_rate"))
        if bitrate == 0 and duration > 0:
            bitrate = int((size * 8) / duration)

        codec = video_stream.get("codec_name", "unknown")

        if size == 0 or duration == 0:
            return None, "Invalid size or duration (0)"

        est_size = estimate_savings(codec, size, crf)

        return VideoStats(
            path=file_path, size_bytes=size, duration_sec=duration,
            bitrate_bps=bitrate, codec=codec, est_converted_size_bytes=est_size
        ), None

    except Exception as e:
        return None, f"Probe error: {str(e)}"

def convert_and_verify(stats: VideoStats, crf: int, delete_original: bool) -> Tuple[bool, Path, str]:
    if stats.codec.lower() == 'hevc':
        return True, stats.path, "Skipped (Already HEVC)"

    # Guardrail 1: Prevent "Disk Full" death spiral (Requires est. size + 2GB buffer)
    free_space = shutil.disk_usage(stats.path.parent).free
    if free_space < (stats.est_converted_size_bytes + (2 * 1024**3)):
        return False, stats.path, "Skipped: Insufficient free disk space"

    # Guardrail 2: Enforce MKV container to safely hold all stream types
    out_path = stats.path.with_suffix(f".hevc_crf{crf}.mkv")

    # Guardrail 3: Map all streams, copy subtitles, force 10-bit color
    cmd = [
        "ffmpeg", "-y", "-i", str(stats.path),
        "-map", "0",
        "-c:v", "libx265", "-crf", str(crf), "-preset", "medium", "-pix_fmt", "yuv420p10le",
        "-c:a", "copy",
        "-c:s", "copy",
        str(out_path)
    ]

    try:
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

        # Sanity Check
        verify_stats, error = probe_video(out_path, crf)
        if not verify_stats:
            raise ValueError(f"Sanity check failed: {error}")

        duration_diff = abs(stats.duration_sec - verify_stats.duration_sec)
        if duration_diff > (stats.duration_sec * 0.01) + 1.0:
            raise ValueError("Sanity check failed: Duration mismatch.")

        if delete_original:
            stats.path.unlink(missing_ok=True)

        return True, stats.path, "Successfully converted"

    except Exception as e:
        out_path.unlink(missing_ok=True) # Safe cleanup on failure
        return False, stats.path, str(e)

# --- Aggregation & Reporting ---

def aggregate_directories(videos: List[VideoStats]) -> List[DirStats]:
    dir_map: Dict[Path, List[VideoStats]] = {}
    for v in videos:
        dir_map.setdefault(v.path.parent, []).append(v)

    aggregates = []
    for directory, vids in dir_map.items():
        codecs = [v.codec for v in vids]
        dominant_codec = Counter(codecs).most_common(1)[0][0]
        bitrates = [v.bitrate_bps for v in vids]

        aggregates.append(DirStats(
            path=directory,
            file_count=len(vids),
            total_size_bytes=sum(v.size_bytes for v in vids),
            total_duration_sec=sum(v.duration_sec for v in vids),
            dominant_codec=dominant_codec,
            est_converted_size_bytes=sum(v.est_converted_size_bytes for v in vids),
            min_bitrate=min(bitrates) if bitrates else 0,
            max_bitrate=max(bitrates) if bitrates else 0,
            avg_bitrate=sum(bitrates) // len(bitrates) if bitrates else 0
        ))
    return aggregates

def format_size(bytes_val: int) -> str:
    return f"{(bytes_val / (1024**3)):.2f} GB"

def render_table(dir_stats: List[DirStats]):
    table = Table(title="\nVideo Directory Analysis (Sorted by Recoverable Space)")
    table.add_column("Directory", style="cyan", no_wrap=True)
    table.add_column("Files", justify="right")
    table.add_column("Codec", style="magenta")
    table.add_column("Avg MB/min", justify="right", style="yellow")
    table.add_column("Current Size", justify="right", style="red")
    table.add_column("Est. Recovered", justify="right", style="bold green")

    for stat in dir_stats:
        table.add_row(
            str(stat.path),
            str(stat.file_count),
            stat.dominant_codec.upper(),
            f"{stat.avg_mb_per_min:.1f}",
            format_size(stat.total_size_bytes),
            format_size(stat.est_recovered_bytes)
        )
    console.print(table)

# --- Main CLI ---

def main():
    if not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
        console.print("[bold red]Error: 'ffmpeg' and 'ffprobe' are required but not found in PATH.[/bold red]")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Analyze and compress video directories safely.")
    parser.add_argument("target", type=Path, help="Target directory to scan")
    parser.add_argument("--crf", type=int, default=28, choices=range(0, 52), metavar="[0-51]", help="Target CRF for H.265 (default: 28)")
    parser.add_argument("--min-savings", type=int, default=10, help="Minimum estimated percentage of space saved to justify conversion (default: 10)")
    parser.add_argument("--verbose", action="store_true", help="Print individual file stats")
    parser.add_argument("--convert", action="store_true", help="Run ffmpeg conversions")
    parser.add_argument("--delete-original", action="store_true", help="Delete original files after passing sanity check")
    parser.add_argument("--probe-workers", type=int, default=max(1, (os.cpu_count() or 2) - 1), help="Threads for fast metadata parsing")
    parser.add_argument("--convert-workers", type=int, default=1, help="Threads for heavy ffmpeg encoding (default: 1 to prevent CPU thrashing)")

    args = parser.parse_args()
    if args.delete_original:
        args.convert = True

    if not args.target.is_dir():
        console.print(f"[bold red]Error: '{args.target}' is not a valid directory.[/bold red]")
        sys.exit(1)

    console.print(f"[bold blue]Scanning '{args.target}' for videos...[/bold blue]")
    video_files = []
    for root, _, files in os.walk(args.target):
        for file in files:
            p = Path(root) / file
            if p.suffix.lower() in VIDEO_EXTENSIONS:
                video_files.append(p)

    if not video_files:
        console.print("[yellow]No video files found.[/yellow]")
        return

    analyzed_videos = []

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TaskProgressColumn()) as progress:
        task = progress.add_task("[cyan]Probing videos...", total=len(video_files))
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.probe_workers) as executor:
            futures = {executor.submit(probe_video, f, args.crf): f for f in video_files}
            for future in concurrent.futures.as_completed(futures):
                stats, error = future.result()
                if stats:
                    analyzed_videos.append(stats)
                elif args.verbose:
                    progress.console.print(f"[red]Failed to probe {futures[future].name}: {error}[/red]")
                progress.advance(task)

    if not analyzed_videos:
        console.print("[bold red]No valid video streams could be parsed.[/bold red]")
        return

    dir_stats = aggregate_directories(analyzed_videos)
    dir_stats.sort(key=lambda x: x.est_recovered_bytes, reverse=True)
    render_table(dir_stats)

    total_recoverable = sum(d.est_recovered_bytes for d in dir_stats)
    console.print(f"\n[bold green]Total Estimated Recoverable Space: {format_size(total_recoverable)}[/bold green]\n")

    if args.convert:
        targets = []
        for v in analyzed_videos:
            if v.codec.lower() == 'hevc':
                continue

            # Guardrail 4: Minimum savings threshold
            savings_pct = (v.size_bytes - v.est_converted_size_bytes) / max(1, v.size_bytes)
            if savings_pct >= (args.min_savings / 100.0):
                targets.append(v)
            elif args.verbose:
                console.print(f"[dim]Skipping {v.path.name}: Estimated savings too low ({savings_pct:.1%})[/dim]")

        if not targets:
            console.print("[green]No files meet the conversion criteria. Nothing to do.[/green]")
            return

        console.print(f"[bold blue]Starting conversion of {len(targets)} files (Min savings: {args.min_savings}%)...[/bold blue]")

        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), BarColumn(), TaskProgressColumn()) as progress:
            task = progress.add_task("[cyan]Converting...", total=len(targets))
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.convert_workers) as executor:
                conv_futures = {executor.submit(convert_and_verify, v, args.crf, args.delete_original): v for v in targets}
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
        # Guardrail 5: Graceful exit to prevent zombie ffmpeg processes
        console.print("\n[bold red]Process interrupted by user. Forcing shutdown...[/bold red]")
        sys.exit(1)