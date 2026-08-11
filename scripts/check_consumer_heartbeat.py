"""Comprueba que el heartbeat del consumidor exista y no esté vencido."""

from __future__ import annotations

import os
import time
from pathlib import Path


def heartbeat_age_seconds(path: Path, now: float | None = None) -> float:
    current = time.time() if now is None else now
    return current - path.stat().st_mtime


def is_heartbeat_healthy(
    path: Path, max_age_seconds: float, now: float | None = None
) -> bool:
    if max_age_seconds < 0 or not path.is_file():
        return False
    try:
        return heartbeat_age_seconds(path, now) <= max_age_seconds
    except OSError:
        return False


def main() -> int:
    path = Path(os.getenv("CONSUMER_HEARTBEAT_FILE", "/tmp/agrostream_consumer_heartbeat"))
    max_age = float(os.getenv("CONSUMER_HEARTBEAT_MAX_AGE_SECONDS", "30"))
    if not is_heartbeat_healthy(path, max_age):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
