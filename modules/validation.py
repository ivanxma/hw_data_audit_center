import re


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]{0,63}", value or ""):
        raise ValueError(
            "Database and table names must be valid identifiers of at most 64 characters."
        )
    return value


def quote(value):
    return "`" + identifier(value).replace("`", "``") + "`"


def bounded(value, default=1, maximum=10000):
    try:
        return max(1, min(int(value), maximum))
    except (TypeError, ValueError):
        return default


def ocid(value, kind):
    if not re.fullmatch(r"ocid1\." + re.escape(kind) + r"\.[A-Za-z0-9._-]+", value or ""):
        raise ValueError(f"A valid {kind} OCID is required.")
    return value
