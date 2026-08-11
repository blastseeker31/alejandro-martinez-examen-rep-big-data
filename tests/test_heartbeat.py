from __future__ import annotations

import os
import time
from pathlib import Path

from scripts.check_consumer_heartbeat import is_heartbeat_healthy


def test_fresh_heartbeat_is_healthy(tmp_path: Path):
    heartbeat = tmp_path / "heartbeat"
    heartbeat.write_text("now", encoding="utf-8")
    now = time.time()

    assert is_heartbeat_healthy(heartbeat, max_age_seconds=30, now=now)


def test_stale_heartbeat_is_unhealthy(tmp_path: Path):
    heartbeat = tmp_path / "heartbeat"
    heartbeat.write_text("old", encoding="utf-8")
    old = time.time() - 61
    os.utime(heartbeat, (old, old))

    assert not is_heartbeat_healthy(heartbeat, max_age_seconds=30, now=time.time())


def test_missing_heartbeat_is_unhealthy(tmp_path: Path):
    assert not is_heartbeat_healthy(tmp_path / "missing", max_age_seconds=30)
