# Video Compression Analyzer

A pragmatic, multi-threaded CLI tool to scan directories for bloated video files, estimate potential storage savings, and optionally batch-convert them to highly efficient H.265 (HEVC) using `ffmpeg`.

This script uses `uv` and PEP 723 inline metadata to manage its Python dependencies (like `rich` for the terminal UI). This means there is no need to manually create virtual environments or run `pip install`—just execute the script, and `uv` handles the rest in an isolated execution environment.

## Prerequisites

This script is designed for Ubuntu/Linux and requires a few system-level tools to be installed.

### 1. Install `ffmpeg` and `ffprobe`
The script relies on `ffprobe` to extract metadata and `ffmpeg` for the actual conversion.
```bash
sudo apt update
sudo apt install ffmpeg
```

### 2. Install `uv`
If you don't already have `uv` (the blazing-fast Python package and project manager), install it via their official script:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
*(You may need to restart your terminal or source your shell configuration after installation.)*

## Usage

Run the script directly via `uv run`. 

```bash
uv run video_analyzer.py [TARGET_DIRECTORY] [OPTIONS]
```

### Arguments and Flags

| Argument/Flag | Type | Description |
| :--- | :--- | :--- |
| `target` | Positional | **Required.** The root directory to recursively scan for video files. |
| `--crf` | Integer | Target Constant Rate Factor for H.265 conversion. Range: 0-51. Lower is better quality, higher is smaller file size. **Default: 28** |
| `--verbose` | Flag | Prints individual file statistics and error messages during the probe and conversion phases. |
| `--convert` | Flag | Executes the `ffmpeg` conversion on non-HEVC files found in the target directory. |
| `--delete-original` | Flag | Deletes the original file **only if** the newly converted file passes a strict `ffprobe` sanity check (valid file + duration match). *Implicitly sets `--convert`.* |
| `--workers` | Integer | Number of concurrent threads for probing and converting. **Default: CPU Cores - 1** |

---

## Examples

### 1. Dry Run (Estimation Only)
Scan a directory and generate a tabular report sorted by the absolute amount of disk space you could recover. This does **not** alter any files.
```bash
uv run video_analyzer.py /mnt/storage/movies
```

### 2. Batch Convert (Keep Originals)
Convert all non-HEVC videos in a directory using the default CRF (28). This creates a new `.hevc_crf28.mp4` file next to the original.
```bash
uv run video_analyzer.py /mnt/storage/movies --convert
```

### 3. Aggressive Space Reclamation (Convert & Delete)
Convert videos using a slightly more aggressive compression (CRF 30). Once a file finishes, run a sanity check on it. If it passes, automatically delete the bloated original file. 
```bash
uv run video_analyzer.py /mnt/storage/tv_shows --crf 30 --delete-original
```

### 4. Maximum CPU Utilization with Verbose Output
Push the parallelization hard (e.g., 16 workers) and print exactly which files are being processed or if any files are failing the probe.
```bash
uv run video_analyzer.py /mnt/storage/raw_captures --convert --workers 16 --verbose
```

## How the Sanity Check Works
If you use the `--delete-original` flag, the script will not blindly delete your source files. After `ffmpeg` finishes creating the new H.265 file, the script runs `ffprobe` on the *new* file. 

The original file is only deleted if:
1. The new file is a valid, readable video format.
2. The duration of the new video matches the original video (within a 1% or 1-second margin of error).

If the sanity check fails, the corrupted output file is deleted, the original file is kept, and the script moves on to the next task.
