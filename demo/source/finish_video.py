"""Mux the recorded walkthrough with narration, captions and chapter metadata."""

import argparse
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
record = json.loads((build / "recording/recording.json").read_text())
if record["errors"]:
    raise RuntimeError("Resolve recorded browser errors before export.")
raw = Path(record["raw"])


def media_duration(path):
    probe = subprocess.run(
        [args.ffmpeg, "-hide_banner", "-i", str(path)], capture_output=True, text=True
    ).stderr
    match = re.search(r"Duration: (\d+):(\d+):(\d+\.\d+)", probe)
    if not match:
        raise RuntimeError("Cannot determine recording duration.")
    return int(match[1]) * 3600 + int(match[2]) * 60 + float(match[3])


duration = media_duration(raw)
trim = max(0, duration - 300)
with wave.open(str(build / "narration.wav"), "rb") as audio:
    if audio.getnframes() != 300 * audio.getframerate():
        raise RuntimeError("Narration must span exactly 300 seconds.")
replacement_file = build / "replacement-global-chart/recording.json"
replacement = json.loads(replacement_file.read_text()) if replacement_file.exists() else None
if replacement:
    if replacement["errors"] or replacement["start"] != 250 or replacement["duration"] != 20:
        raise RuntimeError("Invalid replacement chart recording.")
    replacement_trim = max(0, media_duration(replacement["raw"]) - 20)
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
]
if replacement:
    command.extend(["-ss", str(replacement_trim), "-i", replacement["raw"]])
audio_input = 2 if replacement else 1
command.extend(
    [
        "-i",
        str(build / "narration.wav"),
        "-i",
        str(ROOT / "captions.srt"),
        "-f",
        "ffmetadata",
        "-i",
        str(build / "chapters.ffmeta"),
        "-map",
        "[video]" if replacement else "0:v:0",
        "-map",
        f"{audio_input}:a:0",
        "-map",
        f"{audio_input + 1}:0",
        "-map_metadata",
        str(audio_input + 2),
        "-map_chapters",
        str(audio_input + 2),
        "-t",
        "300",
    ]
)
if replacement:
    command.extend(
        [
            "-filter_complex",
            "[0:v]split=2[a][b];"
            "[a]trim=duration=250,setpts=PTS-STARTPTS[prefix];"
            "[1:v]trim=duration=20,setpts=PTS-STARTPTS[chart];"
            "[b]trim=start=270:duration=30,setpts=PTS-STARTPTS[suffix];"
            "[prefix][chart][suffix]concat=n=3:v=1:a=0,fps=25,format=yuv420p[video]",
        ]
    )
else:
    command.extend(["-vf", "fps=25,format=yuv420p"])
command.extend(
    [
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
)
print("Encoding final MP4; leading recorder time trimmed:", trim, flush=True)
subprocess.run(command, check=True)
info = subprocess.run(
    [args.ffmpeg, "-hide_banner", "-i", str(output)], capture_output=True, text=True
).stderr
(build / "video-metadata.txt").write_text(info)
if (
    "00:05:00.00" not in info
    or "2560x1440" not in info
    or "Audio: aac" not in info
    or "Subtitle: mov_text" not in info
):
    raise RuntimeError("Final media stream validation failed.")
subprocess.run(
    [
        args.ffmpeg,
        "-y",
        "-loglevel",
        "error",
        "-ss",
        "123",
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
verification = dict(
    duration_seconds=300,
    resolution=[2560, 1440],
    fps=25,
    narration="English, installed Daniel voice",
    captions="embedded mov_text, WebVTT and SRT",
    chapters=16,
    browser_errors=record["errors"],
    mode="Illustrative replay; no live OCI or database mutations",
    bytes=output.stat().st_size,
    source_checks="Three DML fixtures use the application event decoder and the same key fingerprint; schema delta uses the application comparison function.",
)
(ROOT / "verification.json").write_text(json.dumps(verification, indent=2) + "\n")
print(json.dumps(verification), flush=True)
