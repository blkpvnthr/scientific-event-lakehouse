"""Measure Bronze build performance over repeated runs.

Reports per-repetition and aggregate timing, output size, compression ratio,
and throughput. Measurements are real; no values are fabricated.

Usage:
    python scripts/benchmark_bronze.py --run-id <run_id> [--reps 3]
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.bronze import build_bronze
from src.spark import build_spark_session, stop_spark_session

# Logical raw payload size (bytes) from the Milestone 2 profile, for the
# compression-ratio reference point.
RAW_PAYLOAD_BYTES = 201_979_748
RECORD_COUNT = 281_421


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark the Bronze build")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--reps", type=int, default=3)
    args = parser.parse_args()

    spark = build_spark_session("bronze-benchmark")
    durations: list[float] = []
    result = None
    spark_version = spark.version
    try:
        for i in range(args.reps):
            result = build_bronze(spark, args.run_id, write_report=(i == args.reps - 1))
            durations.append(result.duration_seconds)
    finally:
        stop_spark_session(spark)

    assert result is not None
    best = min(durations)
    report = {
        "hardware": platform.platform(),
        "python": platform.python_version(),
        "spark": spark_version,
        "run_id": args.run_id,
        "repetitions": args.reps,
        "durations_seconds": durations,
        "min_duration_seconds": best,
        "mean_duration_seconds": round(sum(durations) / len(durations), 3),
        "output_records": result.output_records,
        "output_bytes": result.output_bytes,
        "output_mib": round(result.output_bytes / 1024**2, 2),
        "compression_ratio_vs_raw": round(RAW_PAYLOAD_BYTES / result.output_bytes, 2),
        "throughput_records_per_second": round(result.output_records / best),
        "status": result.status,
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
