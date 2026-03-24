from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class DashboardSettings:
    project_root: Path
    data_dir: Path
    tasks_dir: Path
    overnight_scans_dir: Path
    overnight_reviews_dir: Path
    db_path: Path
    frontend_dir: Path
    frontend_dist: Path
    allowed_origins: list[str]


def get_settings(data_dir: str | Path | None = None) -> DashboardSettings:
    configured_data_dir = (
        Path(data_dir)
        if data_dir is not None
        else Path(
            os.getenv(
                "TRADINGAGENTS_DASHBOARD_DATA_DIR",
                PROJECT_ROOT / "dashboard_data",
            )
        )
    )
    frontend_dir = PROJECT_ROOT / "dashboard-ui"
    allowed_origins = [
        origin.strip()
        for origin in os.getenv(
            "TRADINGAGENTS_DASHBOARD_CORS",
            "http://localhost:5173,http://127.0.0.1:5173",
        ).split(",")
        if origin.strip()
    ]

    return DashboardSettings(
        project_root=PROJECT_ROOT,
        data_dir=configured_data_dir,
        tasks_dir=configured_data_dir / "tasks",
        overnight_scans_dir=configured_data_dir / "overnight_scans",
        overnight_reviews_dir=configured_data_dir / "overnight_reviews",
        db_path=configured_data_dir / "dashboard.db",
        frontend_dir=frontend_dir,
        frontend_dist=frontend_dir / "dist",
        allowed_origins=allowed_origins,
    )
