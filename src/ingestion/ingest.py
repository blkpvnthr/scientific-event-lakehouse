import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from src.ingestion.usgs_client import fetch_earthquakes

BRONZE_PATH = Path("data/bronze")


def ingest(start_date: str, end_date: str) -> Path:

    data = fetch_earthquakes(
        start_date=start_date,
        end_date=end_date,
        limit=1000,
    )

    now = datetime.now(timezone.utc)

    partition = BRONZE_PATH / (f"ingestion_date={now:%Y-%m-%d}")

    partition.mkdir(parents=True, exist_ok=True)

    filename = f"earthquakes_{now:%Y%m%dT%H%M%S}_{uuid4().hex}.json"

    output_path = partition / filename

    with output_path.open("x", encoding="utf-8") as file:
        json.dump(data, file, indent=2)

    print(f"Records ingested: {len(data['features'])}")
    print(f"Saved to: {output_path}")

    return output_path


if __name__ == "__main__":
    ingest(
        start_date="2024-01-01",
        end_date="2024-01-07",
    )
