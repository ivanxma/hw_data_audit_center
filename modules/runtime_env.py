"""Read app-owned launcher defaults as literal assignments, without shell evaluation."""

import re
import shlex


def read_runtime(path):
    values = {}
    if not path.is_file():
        return values
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, raw = line.partition("=")
        if not separator or not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise ValueError("Runtime environment must contain literal KEY=value assignments.")
        tokens = shlex.split(raw, comments=True)
        if len(tokens) > 1:
            raise ValueError("Quote runtime values containing whitespace.")
        value = tokens[0] if tokens else ""
        if any(c in value for c in "\n\r\x00"):
            raise ValueError("Runtime values must be single-line literals.")
        values[key] = value
    return values


def load_runtime_defaults(path, environ):
    for key, value in read_runtime(path).items():
        environ.setdefault(key, value)
