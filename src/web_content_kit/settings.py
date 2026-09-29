"""Turn raw widget / job parameter strings into checked values with friendly error messages."""

import re

_IDENTIFIER = re.compile(r"[A-Za-z0-9_\-]+")


def parse_identifier(name: str, value: str) -> str:
    """A catalog, schema, volume or other object name."""
    value = value.strip()
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"'{name}' may only contain letters, numbers, '_' or '-' (got '{value}').")
    return value


def parse_int(name: str, value: str, minimum: int) -> int:
    """A whole number that is at least ``minimum``."""
    value = value.strip()
    if not value.isdigit() or int(value) < minimum:
        raise ValueError(f"'{name}' must be a whole number >= {minimum} (got '{value}').")
    return int(value)


def parse_float(name: str, value: str, minimum: float = 0.0) -> float:
    """A decimal number that is at least ``minimum``."""
    try:
        number = float(value.strip())
    except ValueError as error:
        raise ValueError(f"'{name}' must be a number, for example 0.5 (got '{value}').") from error
    if number < minimum:
        raise ValueError(f"'{name}' must be >= {minimum} (got '{value}').")
    return number


def parse_bool(value: str) -> bool:
    """``true`` / ``false`` from a dropdown widget."""
    return value.strip().lower() == "true"


def parse_regex(name: str, value: str) -> re.Pattern[str] | None:
    """An optional regular expression (empty means 'not set')."""
    value = value.strip()
    if not value:
        return None
    try:
        return re.compile(value)
    except re.error as error:
        raise ValueError(f"'{name}' is not a valid regular expression: {error}") from error


def parse_urls(name: str, value: str) -> list[str]:
    """One or more http(s) addresses separated by commas, spaces or new lines."""
    urls = [u.strip() for u in re.split(r"[,\s]+", value) if u.strip()]
    if not urls:
        raise ValueError(f"Please enter at least one address in '{name}'.")
    for url in urls:
        if not url.startswith(("http://", "https://")):
            raise ValueError(f"Addresses in '{name}' must start with http:// or https:// (got '{url}').")
    return urls
