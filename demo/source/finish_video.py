"""Mux the reviewed walkthrough with profile narration, captions and chapters."""

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import wave
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--ffmpeg", default=shutil.which("ffmpeg"))
args = parser.parse_args()
if not args.ffmpeg:
    raise SystemExit("Provide --ffmpeg or install ffmpeg on PATH.")
build = ROOT / ".build"
story = json.loads((ROOT / "storyboard.json").read_text())
record = json.loads((build / "recording/recording.json").read_text())
if record["errors"] or record["duration"] != story["duration"]:
    raise ValueError("Recording errors or story duration mismatch; review before export.")
raw = Path(record["raw"])


def probe(path):
    return subprocess.run(
        [args.ffmpeg, "-hide_banner", "-i", str(path)], capture_output=True, text=True
    ).stderr


def media_duration(path):
    match = re.search(r"Duration: (\d+):(\d+):(\d+\.\d+)", probe(path))
    if not match:
        raise ValueError("Cannot determine media duration.")
    return int(match[1]) * 3600 + int(match[2]) * 60 + float(match[3])


total = story["duration"]
trim = max(0, media_duration(raw) - total)
with wave.open(str(build / "narration.wav"), "rb") as audio:
    if audio.getnframes() != total * audio.getframerate():
        raise ValueError("Narration does not span the complete story.")
output = ROOT / "heatwave-audit-oci-streaming-demo.mp4"
command = [
    args.ffmpeg,
    "-y",
    "-hide_banner",
    "-loglevel",
    "warning",
    "-ss",
    str(trim),
    "-i",
    str(raw),
    "-i",
    str(build / "narration.wav"),
    "-i",
    str(ROOT / "captions.srt"),
    "-f",
    "ffmetadata",
    "-i",
    str(build / "chapters.ffmeta"),
    "-map",
    "0:v:0",
    "-map",
    "1:a:0",
    "-map",
    "2:0",
    "-map_metadata",
    "3",
    "-map_chapters",
    "3",
    "-t",
    str(total),
    "-vf",
    "fps=25,format=yuv420p",
    "-c:v",
    "libx264",
    "-preset",
    "fast",
    "-crf",
    "20",
    "-threads",
    "4",
    "-c:a",
    "aac",
    "-b:a",
    "128k",
    "-c:s",
    "mov_text",
    "-metadata:s:s:0",
    "language=eng",
    "-metadata:s:s:0",
    "title=English captions",
    "-movflags",
    "+faststart",
    str(output),
]
print(
    "Encoding reviewed MP4; seconds:", total, "leading recording trim:", round(trim, 3), flush=True
)
subprocess.run(command, check=True)
info = probe(output)
(build / "video-metadata.txt").write_text(info)
if abs(media_duration(output) - total) > 0.15 or any(
    value not in info for value in ["2560x1440", "Audio: aac", "Subtitle: mov_text"]
):
    raise ValueError("Final media validation failed.")
poster_at = next(s["start"] + 6 for s in story["scenes"] if s["id"] == "update")
subprocess.run(
    [
        args.ffmpeg,
        "-y",
        "-loglevel",
        "error",
        "-ss",
        str(poster_at),
        "-i",
        str(output),
        "-frames:v",
        "1",
        "-q:v",
        "2",
        str(ROOT / "assets/poster.jpg"),
    ],
    check=True,
)
digest = hashlib.sha256()
with output.open("rb") as stream:
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        digest.update(chunk)
verification = dict(
    duration_seconds=total,
    resolution=[2560, 1440],
    fps=25,
    narration="English; local AI synthesis conditioned on the supplied consented voice profile",
    captions="embedded mov_text, WebVTT and SRT",
    chapters=len(story["scenes"]),
    browser_errors=record["errors"],
    mode="Illustrative replay with isolated sample data; no live OCI or source mutations",
    bytes=output.stat().st_size,
    sha256=digest.hexdigest(),
    source_checks="DML fixtures use the application event decoder and stable typed-key fingerprints. Runtime behavior was separately validated before material creation.",
)
(ROOT / "verification.json").write_text(json.dumps(verification, indent=2) + "\n")
print(json.dumps(verification), flush=True)
