"""Generate local, profile-conditioned narration, timed captions and chapters.

Requires mlx-audio on a supported Apple Silicon host. The supplied profile stays
local; only synthesized narration is included in the demonstration.
"""

import argparse
import base64
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
import wave
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--ffmpeg", default=shutil.which("ffmpeg"))
parser.add_argument("--voice-profile", default=os.environ.get("AUDIT_VOICE_PROFILE"))
parser.add_argument("--model", default="mlx-community/Qwen3-TTS-12Hz-1.7B-Base-4bit")
args = parser.parse_args()
if not args.ffmpeg or not args.voice_profile:
    raise SystemExit("Provide --ffmpeg and --voice-profile (or AUDIT_VOICE_PROFILE).")
profile = json.loads(Path(args.voice_profile).expanduser().read_text())
recording = profile["recording"]
if not profile.get("consent", {}).get("acceptedAt"):
    raise SystemExit("The voice profile must contain its recorded consent acceptance.")
reference = base64.b64decode(recording["dataBase64"].split(",")[-1], validate=True)
reference_hash = hashlib.sha256(reference).hexdigest()
if reference_hash != recording["sha256"]:
    raise ValueError("Voice profile audio checksum does not match.")
build = ROOT / ".build"
build.mkdir(exist_ok=True)
story = json.loads((ROOT / "storyboard.json").read_text())
model = None


def run(command):
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def duration(path):
    with wave.open(str(path), "rb") as audio:
        return audio.getnframes() / audio.getframerate()


def stamp(seconds, separator="."):
    ms = round(seconds * 1000)
    h, ms = divmod(ms, 3600000)
    m, ms = divmod(ms, 60000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02}{separator}{ms:03}"


# Raw generated clips are reusable only for the same text/model/reference audio.
with tempfile.TemporaryDirectory(prefix="audit-narration-") as temporary:
    reference_path = Path(temporary) / "reference.wav"
    reference_path.write_bytes(reference)
    reference_path.chmod(0o600)
    for i, scene in enumerate(story["scenes"]):
        raw = build / f"{i:02}-profile-raw.wav"
        cache = raw.with_suffix(".json")
        identity = hashlib.sha256(
            json.dumps(
                [scene["narration"], args.model, reference_hash], ensure_ascii=False
            ).encode()
        ).hexdigest()
        cached = json.loads(cache.read_text()) if cache.exists() else {}
        if not raw.exists() or cached.get("identity") != identity:
            import numpy as np
            from mlx_audio.tts.utils import load_model

            if model is None:
                model = load_model(args.model)
            phrases, pending = [], []
            for sentence in re.split(r"(?<=[.!?])\s+", scene["narration"]):
                words = sentence.split()
                if pending and len(pending) + len(words) > 40:
                    phrases.append(" ".join(pending))
                    pending = []
                while len(words) > 40:
                    phrases.append(" ".join(words[:35]))
                    words = words[35:]
                pending += words
            if pending:
                phrases.append(" ".join(pending))
            parts = []
            for phrase in phrases:
                for attempt in range(2):
                    chunks = list(
                        model.generate(
                            text=phrase,
                            ref_audio=str(reference_path),
                            ref_text=recording["script"],
                            lang_code="English",
                            max_tokens=max(160, len(phrase.split()) * 14),
                            temperature=0.65 if attempt == 0 else 0.5,
                            top_p=0.9,
                            repetition_penalty=1.15,
                        )
                    )
                    samples = np.concatenate(
                        [np.asarray(chunk.audio).reshape(-1) for chunk in chunks]
                    )
                    seconds = len(samples) / model.sample_rate
                    if np.isfinite(samples).all() and 1 <= seconds <= max(
                        10, len(phrase.split()) * 0.9 + 3
                    ):
                        break
                else:
                    raise ValueError(
                        "Unexpected speech chunk length; review the scene before continuing."
                    )
                parts.extend([samples, np.zeros(round(model.sample_rate * 0.16))])
            samples = np.concatenate(parts)
            with wave.open(str(raw), "wb") as audio:
                audio.setnchannels(1)
                audio.setsampwidth(2)
                audio.setframerate(model.sample_rate)
                audio.writeframes((np.clip(samples, -1, 1) * 32767).astype(np.int16).tobytes())
            cache.write_text(json.dumps({"identity": identity}) + "\n")
        speech = duration(raw)
        words = len(scene["narration"].split())
        if not 1 <= speech <= max(30, words * 0.9 + 4):
            raise ValueError("Unexpected narration length; review and regenerate this scene.")
        # Preserve natural pacing; extend scenes rather than aggressively speeding speech.
        scene["duration"] = max(scene["duration"], math.ceil(speech / 1.10 + 1.4))
        print(
            json.dumps(
                {
                    "scene": scene["id"],
                    "speech_seconds": round(speech, 2),
                    "segment_seconds": scene["duration"],
                }
            ),
            flush=True,
        )

vtt, srt = ["WEBVTT", ""], []
metadata = [
    ";FFMETADATA1",
    "title=HeatWave Audit Center — native OCI Streaming",
    "comment=Illustrative replay with isolated sample data; synthesized English narration using an operator-supplied voice profile.",
]
timings, number, start = [], 0, 0
for i, scene in enumerate(story["scenes"]):
    scene["start"] = start
    start += scene["duration"]
    raw, out = build / f"{i:02}-profile-raw.wav", build / f"{i:02}-voice.wav"
    speech = duration(raw)
    speed = max(1, speech / (scene["duration"] - 1.4))
    spoken = speech / speed
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
            raise ValueError("Narration segment duration mismatch.")
    words = scene["narration"].split()
    cursor = 0
    for j in range(0, len(words), 11):
        chunk = words[j : j + 11]
        first = scene["start"] + 0.6 + spoken * cursor / len(words)
        cursor += len(chunk)
        last = scene["start"] + 0.6 + spoken * cursor / len(words)
        number += 1
        vtt.extend([f"{stamp(first)} --> {stamp(last)}", " ".join(chunk), ""])
        srt.extend(
            [str(number), f"{stamp(first, ',')} --> {stamp(last, ',')}", " ".join(chunk), ""]
        )
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
    if audio.getnframes() != start * 48000:
        raise ValueError("Concatenated narration duration mismatch.")
story["duration"] = start
(ROOT / "storyboard.json").write_text(json.dumps(story, indent=2) + "\n")
(ROOT / "narration.txt").write_text(
    "\n\n".join(
        f"{s['start'] // 60:02}:{s['start'] % 60:02} — {s['title']}\n{s['narration']}"
        for s in story["scenes"]
    )
    + "\n"
)
(ROOT / "captions.vtt").write_text("\n".join(vtt))
(ROOT / "captions.srt").write_text("\n".join(srt))
(build / "chapters.ffmeta").write_text("\n".join(metadata) + "\n")
(build / "audio-timing.json").write_text(json.dumps(timings, indent=2) + "\n")
print(
    json.dumps(
        {
            "narration_ready": True,
            "scenes": len(timings),
            "seconds": start,
            "voice": "local synthesis conditioned on supplied consented profile",
        }
    ),
    flush=True,
)
