from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard_api.readiness import collect_readiness
from dashboard_api.settings import DashboardSettings


class ReadinessTests(unittest.TestCase):
    def _settings(self, tempdir: str) -> DashboardSettings:
        data_dir = Path(tempdir)
        frontend_dist = data_dir / "dist"
        frontend_dist.mkdir(parents=True, exist_ok=True)
        (frontend_dist / "index.html").write_text("<html></html>", encoding="utf-8")
        return DashboardSettings(
            project_root=data_dir,
            data_dir=data_dir,
            tasks_dir=data_dir / "tasks",
            overnight_scans_dir=data_dir / "overnight_scans",
            overnight_reviews_dir=data_dir / "overnight_reviews",
            db_path=data_dir / "dashboard.db",
            frontend_dir=data_dir,
            frontend_dist=frontend_dist,
            allowed_origins=[],
        )

    def test_collect_readiness_includes_optional_qveris_component(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            settings = self._settings(tempdir)
            with (
                patch("dashboard_api.readiness._check_ark", return_value={"name": "ark", "ok": True, "status": "ok", "message": "ok"}),
                patch("dashboard_api.readiness._check_embedding", return_value={"name": "embedding", "ok": True, "status": "ok", "message": "ok"}),
                patch(
                    "dashboard_api.readiness._check_akshare",
                    return_value={
                        "name": "akshare",
                        "ok": True,
                        "status": "ok",
                        "message": "ok",
                        "checks": {
                            "spot": {"ok": True},
                            "index": {"ok": True},
                            "minute": {"ok": True},
                        },
                    },
                ),
                patch(
                    "dashboard_api.readiness._check_qveris",
                    return_value={
                        "name": "qveris",
                        "ok": False,
                        "status": "error",
                        "message": "not configured",
                        "configured": False,
                        "active_keys": 0,
                        "rotation_enabled": False,
                    },
                ),
                patch("dashboard_api.readiness._check_database", return_value={"name": "database", "ok": True, "status": "ok", "message": "ok"}),
                patch("dashboard_api.readiness._check_frontend", return_value={"name": "frontend", "ok": True, "status": "ok", "message": "ok"}),
            ):
                payload = collect_readiness(settings, refresh=True)

        self.assertTrue(payload["ready"])
        self.assertIn("qveris", payload["components"])
        self.assertFalse(payload["components"]["qveris"]["ok"])
        self.assertEqual(payload["components"]["qveris"]["active_keys"], 0)


if __name__ == "__main__":
    unittest.main()
