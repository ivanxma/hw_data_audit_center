"""Align authored captions to locally reviewed word timestamps."""

import argparse
import bisect
import difflib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser()
parser.add_argument("--transcripts", type=Path, required=True)
args = parser.parse_args()
story = json.loads((ROOT / "storyboard.json").read_text())
timings = {v["id"]: v for v in json.loads((ROOT / ".build/audio-timing.json").read_text())}


def letters(text):
    return re.sub(r"[^a-z0-9]", "", text.lower())


def stamp(seconds, separator="."):
    ms = round(seconds * 1000)
    h, ms = divmod(ms, 3600000)
    m, ms = divmod(ms, 60000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02}{separator}{ms:03}"


vtt, srt, cues = ["WEBVTT", ""], [], []
for scene in story["scenes"]:
    transcript = json.loads((args.transcripts / (scene["id"] + ".json")).read_text())
    if transcript["expected"] != scene["narration"] or transcript["similarity"] < 0.86:
        raise ValueError("Review the current narration transcript before caption alignment.")
    actual, points = "", []
    for segment in transcript["segments"]:
        for word in segment.get("words", []):
            text = letters(word["word"])
            for i, character in enumerate(text):
                actual += character
                points.append(word["start"] + (word["end"] - word["start"]) * i / len(text))
    words = scene["narration"].split()
    expected, starts, ends = "", [], []
    for word in words:
        starts.append(len(expected))
        expected += letters(word)
        ends.append(max(len(expected) - 1, 0))
    known = {}
    for block in difflib.SequenceMatcher(
        None, expected, actual, autojunk=False
    ).get_matching_blocks():
        for i in range(block.size):
            known[block.a + i] = points[block.b + i]
    keys = sorted(known)
    if not keys:
        raise ValueError("Word timestamps could not be aligned.")

    def interpolate(index):
        j = bisect.bisect_left(keys, index)
        if j == 0:
            return known[keys[0]]
        if j == len(keys):
            return known[keys[-1]]
        left, right = keys[j - 1], keys[j]
        return known[left] + (known[right] - known[left]) * (index - left) / (right - left)

    speed = timings[scene["id"]]["speed"]
    previous = scene["start"]
    for i in range(0, len(words), 11):
        j = min(i + 11, len(words))
        start = max(previous, scene["start"] + 0.6 + interpolate(starts[i]) / speed)
        end = min(
            scene["start"] + scene["duration"] - 0.1,
            max(start + 0.25, scene["start"] + 0.6 + (interpolate(ends[j - 1]) + 0.07) / speed),
        )
        if end <= start:
            raise ValueError("Invalid caption cue boundary.")
        previous = end
        text = " ".join(words[i:j])
        number = len(cues) + 1
        vtt.extend([f"{stamp(start)} --> {stamp(end)}", text, ""])
        srt.extend([str(number), f"{stamp(start, ',')} --> {stamp(end, ',')}", text, ""])
        cues.append({"scene": scene["id"], "start": round(start, 3), "end": round(end, 3)})
(ROOT / "captions.vtt").write_text("\n".join(vtt))
(ROOT / "captions.srt").write_text("\n".join(srt))
(ROOT / ".build/caption-alignment.json").write_text(
    json.dumps({"cues": cues, "local_word_timestamps": True}, indent=2) + "\n"
)
print(
    json.dumps(
        {"aligned_cues": len(cues), "scenes": len(story["scenes"]), "seconds": story["duration"]}
    )
)
