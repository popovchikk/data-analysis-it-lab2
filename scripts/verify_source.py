from __future__ import annotations

import csv
from pathlib import Path

SOURCE = Path("data/source/hour.csv")
REQUIRED_COLUMNS = {
    "instant",
    "dteday",
    "season",
    "yr",
    "mnth",
    "hr",
    "holiday",
    "weekday",
    "workingday",
    "weathersit",
    "temp",
    "atemp",
    "hum",
    "windspeed",
    "casual",
    "registered",
    "cnt",
}


def main() -> None:
    if not SOURCE.exists():
        raise SystemExit(
            "Файл data/source/hour.csv не найден. "
            "Сначала выполните: uv run python scripts/download_source.py"
        )

    with SOURCE.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - columns
        if missing:
            raise SystemExit(f"Не хватает столбцов: {sorted(missing)}")
        first_row = next(reader, None)
        if first_row is None:
            raise SystemExit("hour.csv пустой")

    print("Source check: OK")
    print(f"Path: {SOURCE}")
    print(f"Columns: {len(columns)}")
    print(f"Required columns present: {sorted(REQUIRED_COLUMNS)}")
    print(f"First row: {first_row}")


if __name__ == "__main__":
    main()
