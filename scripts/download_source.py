from __future__ import annotations

import os
from pathlib import Path

import requests

# The UCI endpoint from the original scaffold returned HTTP 404 during the lab run.
# The assignment explicitly allows a GitHub raw fallback pinned to a commit SHA.
DEFAULT_SOURCE_URL = (
    "https://raw.githubusercontent.com/alfozan/mlflow-example/"
    "fa1ed37c09962d5b134d07703ba95f07648b73e0/data/hour.csv"
)
SOURCE_URL = os.getenv("SOURCE_URL", DEFAULT_SOURCE_URL)
OUTPUT = Path("data/source/hour.csv")


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    response = requests.get(SOURCE_URL, timeout=60)
    response.raise_for_status()
    OUTPUT.write_bytes(response.content)
    print(f"Saved {len(response.content)} bytes from {SOURCE_URL} to {OUTPUT}")


if __name__ == "__main__":
    main()
