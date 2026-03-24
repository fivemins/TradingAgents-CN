from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


class TaskLauncher:
    def __init__(
        self,
        project_root: str | Path,
        data_dir: str | Path,
        python_executable: str | None = None,
    ):
        self.project_root = Path(project_root)
        self.data_dir = Path(data_dir)
        self.python_executable = python_executable or sys.executable

    def _spawn(self, module_name: str, identifier: str) -> int:
        env = os.environ.copy()
        env["TRADINGAGENTS_DASHBOARD_DATA_DIR"] = str(self.data_dir)
        self._ensure_local_no_proxy(env)
        command = [self.python_executable, "-m", module_name, identifier]
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

        process = subprocess.Popen(
            command,
            cwd=self.project_root,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        return int(process.pid)

    @staticmethod
    def _ensure_local_no_proxy(env: dict[str, str]) -> None:
        local_hosts = ["127.0.0.1", "localhost"]
        for key in ("NO_PROXY", "no_proxy"):
            current = env.get(key, "")
            items = [item.strip() for item in current.split(",") if item.strip()]
            for host in local_hosts:
                if host not in items:
                    items.append(host)
            env[key] = ",".join(items)

    def launch(self, task_id: str) -> int:
        return self.launch_task(task_id)

    def launch_task(self, task_id: str) -> int:
        return self._spawn("dashboard_api.runner", task_id)

    def launch_overnight_scan(self, scan_id: str) -> int:
        return self._spawn("dashboard_api.overnight_runner", scan_id)

    def launch_overnight_review(self, review_id: str) -> int:
        return self._spawn("dashboard_api.overnight_review_runner", review_id)
