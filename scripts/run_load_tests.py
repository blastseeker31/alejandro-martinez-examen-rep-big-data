"""Ejecuta lotes reales contra la API y guarda respuestas sin inventar métricas."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import requests


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", nargs="+", type=int, default=[100, 1000, 5000, 10000])
    parser.add_argument("--scenario", default="mixed")
    parser.add_argument("--seed", type=int, default=20260811)
    parser.add_argument("--anomaly-percent", type=float, default=15)
    parser.add_argument("--duplicate-percent", type=float, default=0)
    parser.add_argument("--malformed-percent", type=float, default=0)
    parser.add_argument("--output", default="artifacts/runtime/load_test_results.json")
    args = parser.parse_args()
    base_url = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
    results = []
    for size in args.sizes:
        payload = {
            "sizes": [size],
            "scenario": args.scenario,
            "seed": args.seed,
            "anomaly_percent": args.anomaly_percent,
            "duplicate_percent": args.duplicate_percent,
            "malformed_percent": args.malformed_percent,
        }
        started = time.monotonic()
        response = requests.post(f"{base_url}/events/load-test", json=payload, timeout=600)
        response.raise_for_status()
        result = response.json()["runs"][0]
        result["client_elapsed_seconds"] = round(time.monotonic() - started, 6)
        result["executed_at"] = datetime.now(UTC).isoformat()
        results.append(result)
        print(json.dumps(result, ensure_ascii=False))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"results_file={output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
