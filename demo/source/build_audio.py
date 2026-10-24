"""Render installed-voice narration and timed captions for the demo."""

import argparse
import json
import shutil
import subprocess
import wave
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--ffmpeg", default=shutil.which("ffmpeg"))
parser.add_argument("--voice", default="Daniel")
args = parser.parse_args()
if not args.ffmpeg:
    raise SystemExit("Provide --ffmpeg or install ffmpeg on PATH.")
build = ROOT / ".build"
build.mkdir(exist_ok=True)
story = json.loads((ROOT / "storyboard.json").read_text())
vtt = ["WEBVTT", ""]
srt = []
metadata = [
    ";FFMETADATA1",
    "title=HeatWave Audit Center — OCI Streaming demonstration",
    "comment=Illustrative replay with isolated demo data; not a live acceptance test.",
]
timings = []
number = 0


def run(command):
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def stamp(seconds, separator="."):
    ms = round(seconds * 1000)
    h, ms = divmod(ms, 3600000)
    m, ms = divmod(ms, 60000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02}{separator}{ms:03}"


for i, scene in enumerate(story["scenes"]):
    text = build / f"{i:02}-voice.txt"
    text.write_text(scene["narration"])
    aiff = build / f"{i:02}-voice.aiff"
    raw = build / f"{i:02}-raw.wav"
    out = build / f"{i:02}-voice.wav"
    run(["/usr/bin/say", "-v", args.voice, "-r", "165", "-f", str(text), "-o", str(aiff)])
    run(
        [
            args.ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-i",
            str(aiff),
            "-ar",
            "48000",
            "-ac",
            "1",
            str(raw),
        ]
    )
    with wave.open(str(raw), "rb") as audio:
        duration = audio.getnframes() / audio.getframerate()
    if duration < 1:
        raise RuntimeError(
            "Speech service produced empty audio; run with access to the installed speech service."
        )
    speed = max(1, duration / (scene["duration"] - 1.4))
    spoken = duration / speed
    filters = f"atempo={speed:.6f},loudnorm=I=-18:TP=-2:LRA=7,aresample=48000,asetpts=N/SR/TB,adelay=600,apad=whole_len={scene['duration'] * 48000},atrim=end_sample={scene['duration'] * 48000},asetpts=N/SR/TB"
    run(
        [
            args.ffmpeg,
            "-y",
            "-loglevel",
            "error",
            "-i",
            str(raw),
            "-af",
            filters,
            "-ar",
            "48000",
            "-ac",
            "1",
            str(out),
        ]
    )
    with wave.open(str(out), "rb") as audio:
        if audio.getnframes() != scene["duration"] * 48000:
            raise RuntimeError(f"Narration segment duration mismatch: {scene['id']}")
    words = scene["narration"].split()
    chunks = [words[j : j + 11] for j in range(0, len(words), 11)]
    cursor = 0
    for chunk in chunks:
        start = scene["start"] + 0.6 + spoken * cursor / len(words)
        cursor += len(chunk)
        end = scene["start"] + 0.6 + spoken * cursor / len(words)
        number += 1
        text = " ".join(chunk)
        vtt.extend([f"{stamp(start)} --> {stamp(end)}", text, ""])
        srt.extend([str(number), f"{stamp(start, ',')} --> {stamp(end, ',')}", text, ""])
    metadata.extend(
        [
            "[CHAPTER]",
            "TIMEBASE=1/1000",
            f"START={scene['start'] * 1000}",
            f"END={(scene['start'] + scene['duration']) * 1000}",
            "title=" + scene["title"],
        ]
    )
    timings.append(
        dict(
            id=scene["id"],
            speech_seconds=round(spoken, 3),
            segment_seconds=scene["duration"],
            speed=round(speed, 3),
        )
    )
    print(scene["id"], round(spoken, 1), "seconds", flush=True)
concat = build / "audio.ffconcat"
concat.write_text(
    "ffconcat version 1.0\n"
    + "".join(f"file '{i:02}-voice.wav'\n" for i in range(len(story["scenes"])))
)
run(
    [
        args.ffmpeg,
        "-y",
        "-loglevel",
        "error",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat),
        "-c:a",
        "pcm_s16le",
        str(build / "narration.wav"),
    ]
)
with wave.open(str(build / "narration.wav"), "rb") as audio:
    expected_frames = sum(scene["duration"] for scene in story["scenes"]) * 48000
    if audio.getnframes() != expected_frames:
        raise RuntimeError("Concatenated narration duration mismatch.")
(ROOT / "captions.vtt").write_text("\n".join(vtt))
(ROOT / "captions.srt").write_text("\n".join(srt))
(build / "chapters.ffmeta").write_text("\n".join(metadata) + "\n")
(build / "audio-timing.json").write_text(json.dumps(timings, indent=2) + "\n")
print("Narration and captions ready.", flush=True)
