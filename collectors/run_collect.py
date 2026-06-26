import os
from pathlib import Path

from dotenv import load_dotenv

from collectors.seoul_citydata import collect_all, load_areas

PROJECT_ROOT = Path(__file__).resolve().parent.parent
AREAS_FILE = Path(__file__).resolve().parent / "areas.txt"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def main() -> None:
    load_dotenv()
    api_key = os.environ["SEOUL_API_KEY"]
    area_names = load_areas(AREAS_FILE)

    saved_paths = collect_all(area_names=area_names, api_key=api_key, base_dir=RAW_DATA_DIR)

    print(f"{len(saved_paths)}개 장소 수집 완료 -> {RAW_DATA_DIR}")


if __name__ == "__main__":
    main()
