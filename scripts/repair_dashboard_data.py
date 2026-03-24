from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard_api.repair import repair_dashboard_data


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair and backfill dashboard data records.")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Optional dashboard data directory. Defaults to TRADINGAGENTS_DASHBOARD_DATA_DIR or dashboard_data.",
    )
    args = parser.parse_args()
    report = repair_dashboard_data(args.data_dir)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
