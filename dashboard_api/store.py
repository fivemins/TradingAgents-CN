from __future__ import annotations

from contextlib import closing
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TASK_COLUMNS = [
    "task_id",
    "ticker",
    "analysis_date",
    "market_region",
    "analysts",
    "research_depth",
    "llm_provider",
    "quick_think_llm",
    "deep_think_llm",
    "online_tools",
    "status",
    "stage",
    "progress_message",
    "config_snapshot",
    "structured_summary",
    "artifact_dir",
    "decision",
    "error_message",
    "created_at",
    "started_at",
    "finished_at",
    "pid",
]

SCAN_COLUMNS = [
    "scan_id",
    "trade_date",
    "market_region",
    "mode",
    "status",
    "progress_message",
    "summary_json",
    "market_message",
    "formal_count",
    "watchlist_count",
    "artifact_dir",
    "error_message",
    "created_at",
    "started_at",
    "finished_at",
    "pid",
]

REVIEW_COLUMNS = [
    "review_id",
    "end_trade_date",
    "market_region",
    "window_days",
    "mode",
    "return_basis",
    "status",
    "progress_message",
    "summary_json",
    "artifact_dir",
    "error_message",
    "created_at",
    "started_at",
    "finished_at",
    "pid",
]

CANDIDATE_COLUMNS = [
    "candidate_id",
    "scan_id",
    "trade_date",
    "market_region",
    "bucket",
    "ticker",
    "name",
    "pool",
    "quality",
    "latest",
    "pct",
    "amount",
    "turnover",
    "quick_score",
    "total_score",
    "factor_breakdown",
    "tail_metrics",
    "filter_reason",
    "excluded_from_final",
    "linked_task_id",
    "validation_status",
    "next_open_return",
    "next_open_date",
    "scan_close_price",
    "created_at",
    "updated_at",
]

TRACKED_TRADE_COLUMNS = [
    "trade_id",
    "trade_date",
    "market_region",
    "scan_id",
    "scan_mode",
    "source_bucket",
    "ticker",
    "name",
    "pool",
    "quality",
    "quick_score",
    "total_score",
    "factor_breakdown",
    "tail_metrics",
    "confirmed_at",
    "entry_target_time",
    "entry_price",
    "entry_time_used",
    "exit_target_time",
    "exit_trade_date",
    "exit_price",
    "exit_time_used",
    "strategy_return",
    "status",
    "last_error",
    "last_checked_at",
    "created_at",
    "updated_at",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    return connection


class TaskStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    analysis_date TEXT NOT NULL,
                    market_region TEXT NOT NULL DEFAULT 'cn_a',
                    analysts TEXT NOT NULL,
                    research_depth INTEGER NOT NULL,
                    llm_provider TEXT NOT NULL,
                    quick_think_llm TEXT NOT NULL,
                    deep_think_llm TEXT NOT NULL,
                    online_tools INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    progress_message TEXT NOT NULL,
                    config_snapshot TEXT NOT NULL,
                    structured_summary TEXT,
                    artifact_dir TEXT NOT NULL,
                    decision TEXT,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    pid INTEGER
                )
                """
            )
            self._migrate_schema(conn)
            conn.commit()

    def create_task(
        self,
        task_id: str,
        payload: dict[str, Any],
        artifact_dir: Path,
        config_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        record = {
            "task_id": task_id,
            "ticker": payload["ticker"],
            "analysis_date": payload["analysis_date"],
            "market_region": payload["market_region"],
            "analysts": payload["analysts"],
            "research_depth": payload["research_depth"],
            "llm_provider": payload["llm_provider"],
            "quick_think_llm": payload["quick_think_llm"],
            "deep_think_llm": payload["deep_think_llm"],
            "online_tools": payload["online_tools"],
            "status": "queued",
            "stage": "initializing",
            "progress_message": "Task created and waiting to start.",
            "config_snapshot": config_snapshot,
            "structured_summary": None,
            "artifact_dir": str(artifact_dir),
            "decision": None,
            "error_message": None,
            "created_at": utc_now(),
            "started_at": None,
            "finished_at": None,
            "pid": None,
        }
        db_record = self._encode_record(record)
        placeholders = ", ".join("?" for _ in TASK_COLUMNS)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"INSERT INTO tasks ({', '.join(TASK_COLUMNS)}) VALUES ({placeholders})",
                [db_record[column] for column in TASK_COLUMNS],
            )
            conn.commit()
        return record

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        with closing(_connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
        return self._decode_row(row) if row else None

    def list_tasks(
        self,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM tasks"
        params: list[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with closing(_connect(self.db_path)) as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._decode_row(row) for row in rows]

    def update_task(self, task_id: str, **fields: Any) -> dict[str, Any] | None:
        if not fields:
            return self.get_task(task_id)

        encoded = self._encode_record(fields)
        assignments = ", ".join(f"{column} = ?" for column in encoded.keys())
        values = list(encoded.values()) + [task_id]
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"UPDATE tasks SET {assignments} WHERE task_id = ?",
                values,
            )
            conn.commit()
        return self.get_task(task_id)

    def delete_task(self, task_id: str) -> None:
        with closing(_connect(self.db_path)) as conn:
            conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            conn.commit()

    def get_stats(self) -> dict[str, int]:
        stats = {"total": 0, "queued": 0, "running": 0, "succeeded": 0, "failed": 0}
        with closing(_connect(self.db_path)) as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM tasks GROUP BY status"
            ).fetchall()
            total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        stats["total"] = int(total)
        for row in rows:
            stats[row["status"]] = int(row["count"])
        return stats

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(tasks)").fetchall()
        }
        if "market_region" not in existing_columns:
            conn.execute(
                "ALTER TABLE tasks ADD COLUMN market_region TEXT NOT NULL DEFAULT 'cn_a'"
            )
        if "structured_summary" not in existing_columns:
            conn.execute("ALTER TABLE tasks ADD COLUMN structured_summary TEXT")

    def _encode_record(self, record: dict[str, Any]) -> dict[str, Any]:
        encoded: dict[str, Any] = {}
        for key, value in record.items():
            if key in {"analysts", "config_snapshot", "structured_summary"}:
                encoded[key] = json.dumps(value)
            elif key == "online_tools":
                encoded[key] = int(bool(value))
            else:
                encoded[key] = value
        return encoded

    def _decode_row(self, row: sqlite3.Row) -> dict[str, Any]:
        task = dict(row)
        task["analysts"] = json.loads(task["analysts"])
        task["config_snapshot"] = json.loads(task["config_snapshot"])
        task["structured_summary"] = (
            json.loads(task["structured_summary"])
            if task.get("structured_summary")
            else None
        )
        task["online_tools"] = bool(task["online_tools"])
        return task


class OvernightScanStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS overnight_scans (
                    scan_id TEXT PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    market_region TEXT NOT NULL DEFAULT 'cn_a',
                    mode TEXT NOT NULL,
                    status TEXT NOT NULL,
                    progress_message TEXT NOT NULL,
                    summary_json TEXT,
                    market_message TEXT NOT NULL,
                    formal_count INTEGER NOT NULL DEFAULT 0,
                    watchlist_count INTEGER NOT NULL DEFAULT 0,
                    artifact_dir TEXT NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    pid INTEGER
                )
                """
            )
            self._migrate_schema(conn)
            conn.commit()

    def create_scan(
        self,
        scan_id: str,
        payload: dict[str, Any],
        artifact_dir: Path,
    ) -> dict[str, Any]:
        record = {
            "scan_id": scan_id,
            "trade_date": payload["trade_date"],
            "market_region": payload["market_region"],
            "mode": payload["mode"],
            "status": "queued",
            "progress_message": "Scan created and waiting to start.",
            "summary_json": None,
            "market_message": "Waiting for market scan to start.",
            "formal_count": 0,
            "watchlist_count": 0,
            "artifact_dir": str(artifact_dir),
            "error_message": None,
            "created_at": utc_now(),
            "started_at": None,
            "finished_at": None,
            "pid": None,
        }
        db_record = self._encode_record(record)
        placeholders = ", ".join("?" for _ in SCAN_COLUMNS)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"INSERT INTO overnight_scans ({', '.join(SCAN_COLUMNS)}) VALUES ({placeholders})",
                [db_record[column] for column in SCAN_COLUMNS],
            )
            conn.commit()
        return record

    def get_scan(self, scan_id: str) -> dict[str, Any] | None:
        with closing(_connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM overnight_scans WHERE scan_id = ?",
                (scan_id,),
            ).fetchone()
        return self._decode_row(row) if row else None

    def list_scans(
        self,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM overnight_scans"
        params: list[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with closing(_connect(self.db_path)) as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._decode_row(row) for row in rows]

    def update_scan(self, scan_id: str, **fields: Any) -> dict[str, Any] | None:
        if not fields:
            return self.get_scan(scan_id)

        encoded = self._encode_record(fields)
        assignments = ", ".join(f"{column} = ?" for column in encoded.keys())
        values = list(encoded.values()) + [scan_id]
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"UPDATE overnight_scans SET {assignments} WHERE scan_id = ?",
                values,
            )
            conn.commit()
        return self.get_scan(scan_id)

    def delete_scan(self, scan_id: str) -> None:
        with closing(_connect(self.db_path)) as conn:
            conn.execute("DELETE FROM overnight_scans WHERE scan_id = ?", (scan_id,))
            conn.commit()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(overnight_scans)").fetchall()
        }
        if "summary_json" not in existing_columns:
            conn.execute("ALTER TABLE overnight_scans ADD COLUMN summary_json TEXT")
        if "market_region" not in existing_columns:
            conn.execute(
                "ALTER TABLE overnight_scans ADD COLUMN market_region TEXT NOT NULL DEFAULT 'cn_a'"
            )
        if "market_message" not in existing_columns:
            conn.execute(
                "ALTER TABLE overnight_scans ADD COLUMN market_message TEXT NOT NULL DEFAULT ''"
            )
        if "formal_count" not in existing_columns:
            conn.execute(
                "ALTER TABLE overnight_scans ADD COLUMN formal_count INTEGER NOT NULL DEFAULT 0"
            )
        if "watchlist_count" not in existing_columns:
            conn.execute(
                "ALTER TABLE overnight_scans ADD COLUMN watchlist_count INTEGER NOT NULL DEFAULT 0"
            )

    def _encode_record(self, record: dict[str, Any]) -> dict[str, Any]:
        encoded: dict[str, Any] = {}
        for key, value in record.items():
            if key == "summary_json":
                encoded[key] = json.dumps(value) if value is not None else None
            else:
                encoded[key] = value
        return encoded

    def _decode_row(self, row: sqlite3.Row) -> dict[str, Any]:
        scan = dict(row)
        scan["summary_json"] = (
            json.loads(scan["summary_json"]) if scan.get("summary_json") else None
        )
        return scan


class OvernightReviewStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS overnight_reviews (
                    review_id TEXT PRIMARY KEY,
                    end_trade_date TEXT NOT NULL,
                    market_region TEXT NOT NULL DEFAULT 'cn_a',
                    window_days INTEGER NOT NULL DEFAULT 60,
                    mode TEXT NOT NULL DEFAULT 'strict',
                    return_basis TEXT NOT NULL DEFAULT 'buy_1455_sell_next_day_1000',
                    status TEXT NOT NULL,
                    progress_message TEXT NOT NULL,
                    summary_json TEXT,
                    artifact_dir TEXT NOT NULL,
                    error_message TEXT,
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    pid INTEGER
                )
                """
            )
            self._migrate_schema(conn)
            conn.commit()

    def create_review(
        self,
        review_id: str,
        payload: dict[str, Any],
        artifact_dir: Path,
    ) -> dict[str, Any]:
        record = {
            "review_id": review_id,
            "end_trade_date": payload["end_trade_date"],
            "market_region": payload["market_region"],
            "window_days": payload.get("window_days", 60),
            "mode": payload.get("mode", "strict"),
            "return_basis": payload.get("return_basis", "buy_1455_sell_next_day_1000"),
            "status": "queued",
            "progress_message": "Review created and waiting to start.",
            "summary_json": None,
            "artifact_dir": str(artifact_dir),
            "error_message": None,
            "created_at": utc_now(),
            "started_at": None,
            "finished_at": None,
            "pid": None,
        }
        db_record = self._encode_record(record)
        placeholders = ", ".join("?" for _ in REVIEW_COLUMNS)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"INSERT INTO overnight_reviews ({', '.join(REVIEW_COLUMNS)}) VALUES ({placeholders})",
                [db_record[column] for column in REVIEW_COLUMNS],
            )
            conn.commit()
        return record

    def get_review(self, review_id: str) -> dict[str, Any] | None:
        with closing(_connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM overnight_reviews WHERE review_id = ?",
                (review_id,),
            ).fetchone()
        return self._decode_row(row) if row else None

    def list_reviews(
        self,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM overnight_reviews"
        params: list[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        with closing(_connect(self.db_path)) as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._decode_row(row) for row in rows]

    def update_review(self, review_id: str, **fields: Any) -> dict[str, Any] | None:
        if not fields:
            return self.get_review(review_id)

        encoded = self._encode_record(fields)
        assignments = ", ".join(f"{column} = ?" for column in encoded.keys())
        values = list(encoded.values()) + [review_id]
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"UPDATE overnight_reviews SET {assignments} WHERE review_id = ?",
                values,
            )
            conn.commit()
        return self.get_review(review_id)

    def delete_review(self, review_id: str) -> None:
        with closing(_connect(self.db_path)) as conn:
            conn.execute("DELETE FROM overnight_reviews WHERE review_id = ?", (review_id,))
            conn.commit()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(overnight_reviews)").fetchall()
        }
        if "market_region" not in existing_columns:
            conn.execute(
                "ALTER TABLE overnight_reviews ADD COLUMN market_region TEXT NOT NULL DEFAULT 'cn_a'"
            )
        if "window_days" not in existing_columns:
            conn.execute(
                "ALTER TABLE overnight_reviews ADD COLUMN window_days INTEGER NOT NULL DEFAULT 60"
            )
        if "mode" not in existing_columns:
            conn.execute(
                "ALTER TABLE overnight_reviews ADD COLUMN mode TEXT NOT NULL DEFAULT 'strict'"
            )
        if "return_basis" not in existing_columns:
            conn.execute(
                    "ALTER TABLE overnight_reviews ADD COLUMN return_basis TEXT NOT NULL DEFAULT 'buy_1455_sell_next_day_1000'"
            )
        if "summary_json" not in existing_columns:
            conn.execute("ALTER TABLE overnight_reviews ADD COLUMN summary_json TEXT")

    def _encode_record(self, record: dict[str, Any]) -> dict[str, Any]:
        encoded: dict[str, Any] = {}
        for key, value in record.items():
            if key == "summary_json":
                encoded[key] = json.dumps(value) if value is not None else None
            else:
                encoded[key] = value
        return encoded

    def _decode_row(self, row: sqlite3.Row) -> dict[str, Any]:
        review = dict(row)
        review["summary_json"] = (
            json.loads(review["summary_json"]) if review.get("summary_json") else None
        )
        return review


class OvernightCandidateStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS overnight_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    scan_id TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    market_region TEXT NOT NULL DEFAULT 'cn_a',
                    bucket TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    name TEXT NOT NULL,
                    pool TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    latest REAL NOT NULL,
                    pct REAL NOT NULL,
                    amount REAL NOT NULL,
                    turnover REAL NOT NULL,
                    quick_score REAL NOT NULL,
                    total_score REAL NOT NULL,
                    factor_breakdown TEXT NOT NULL,
                    tail_metrics TEXT,
                    filter_reason TEXT,
                    excluded_from_final TEXT,
                    linked_task_id TEXT,
                    validation_status TEXT,
                    next_open_return REAL,
                    next_open_date TEXT,
                    scan_close_price REAL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._migrate_schema(conn)
            conn.commit()

    def replace_scan_candidates(
        self,
        scan_id: str,
        trade_date: str,
        market_region: str,
        formal_candidates: list[dict[str, Any]],
        watchlist_candidates: list[dict[str, Any]],
    ) -> None:
        rows: list[dict[str, Any]] = []
        now = utc_now()
        for bucket, candidates in (
            ("formal", formal_candidates),
            ("watchlist", watchlist_candidates),
        ):
            for candidate in candidates:
                rows.append(
                    {
                        "candidate_id": self._candidate_id(scan_id, candidate["ticker"]),
                        "scan_id": scan_id,
                        "trade_date": trade_date,
                        "market_region": market_region,
                        "bucket": bucket,
                        "ticker": candidate["ticker"],
                        "name": candidate["name"],
                        "pool": candidate["pool"],
                        "quality": candidate["quality"],
                        "latest": candidate["latest"],
                        "pct": candidate["pct"],
                        "amount": candidate["amount"],
                        "turnover": candidate["turnover"],
                        "quick_score": candidate["quick_score"],
                        "total_score": candidate["total_score"],
                        "factor_breakdown": candidate.get("factor_breakdown") or {},
                        "tail_metrics": candidate.get("tail_metrics"),
                        "filter_reason": candidate.get("filter_reason"),
                        "excluded_from_final": candidate.get("excluded_from_final"),
                        "linked_task_id": None,
                        "validation_status": "pending" if bucket == "formal" else "watchlist_only",
                        "next_open_return": None,
                        "next_open_date": None,
                        "scan_close_price": candidate.get("latest"),
                        "created_at": now,
                        "updated_at": now,
                    }
                )

        with closing(_connect(self.db_path)) as conn:
            conn.execute("DELETE FROM overnight_candidates WHERE scan_id = ?", (scan_id,))
            if rows:
                placeholders = ", ".join("?" for _ in CANDIDATE_COLUMNS)
                conn.executemany(
                    f"INSERT INTO overnight_candidates ({', '.join(CANDIDATE_COLUMNS)}) VALUES ({placeholders})",
                    [
                        [self._encode_record(row)[column] for column in CANDIDATE_COLUMNS]
                        for row in rows
                    ],
                )
            conn.commit()

    def get_candidate(self, scan_id: str, ticker: str) -> dict[str, Any] | None:
        with closing(_connect(self.db_path)) as conn:
            row = conn.execute(
                """
                SELECT * FROM overnight_candidates
                WHERE scan_id = ? AND ticker = ?
                """,
                (scan_id, ticker),
            ).fetchone()
        return self._decode_row(row) if row else None

    def list_candidates(self, scan_id: str) -> list[dict[str, Any]]:
        with closing(_connect(self.db_path)) as conn:
            rows = conn.execute(
                """
                SELECT * FROM overnight_candidates
                WHERE scan_id = ?
                ORDER BY CASE bucket WHEN 'formal' THEN 0 ELSE 1 END, total_score DESC
                """,
                (scan_id,),
            ).fetchall()
        return [self._decode_row(row) for row in rows]

    def delete_scan_candidates(self, scan_id: str) -> None:
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                "DELETE FROM overnight_candidates WHERE scan_id = ?",
                (scan_id,),
            )
            conn.commit()

    def link_task(self, scan_id: str, ticker: str, task_id: str) -> dict[str, Any] | None:
        return self.update_candidate(
            scan_id,
            ticker,
            linked_task_id=task_id,
            updated_at=utc_now(),
        )

    def update_candidate(
        self,
        scan_id: str,
        ticker: str,
        **fields: Any,
    ) -> dict[str, Any] | None:
        if not fields:
            return self.get_candidate(scan_id, ticker)

        encoded = self._encode_record(fields)
        assignments = ", ".join(f"{column} = ?" for column in encoded.keys())
        values = list(encoded.values()) + [scan_id, ticker]
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"""
                UPDATE overnight_candidates
                SET {assignments}
                WHERE scan_id = ? AND ticker = ?
                """,
                values,
            )
            conn.commit()
        return self.get_candidate(scan_id, ticker)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(overnight_candidates)").fetchall()
        }
        migrations = {
            "market_region": "ALTER TABLE overnight_candidates ADD COLUMN market_region TEXT NOT NULL DEFAULT 'cn_a'",
            "linked_task_id": "ALTER TABLE overnight_candidates ADD COLUMN linked_task_id TEXT",
            "validation_status": "ALTER TABLE overnight_candidates ADD COLUMN validation_status TEXT",
            "next_open_return": "ALTER TABLE overnight_candidates ADD COLUMN next_open_return REAL",
            "next_open_date": "ALTER TABLE overnight_candidates ADD COLUMN next_open_date TEXT",
            "scan_close_price": "ALTER TABLE overnight_candidates ADD COLUMN scan_close_price REAL",
            "created_at": "ALTER TABLE overnight_candidates ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
            "updated_at": "ALTER TABLE overnight_candidates ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in migrations.items():
            if column not in existing_columns:
                conn.execute(statement)

    def _candidate_id(self, scan_id: str, ticker: str) -> str:
        return f"{scan_id}:{ticker}"

    def _encode_record(self, record: dict[str, Any]) -> dict[str, Any]:
        encoded: dict[str, Any] = {}
        for key, value in record.items():
            if key in {"factor_breakdown", "tail_metrics"}:
                encoded[key] = json.dumps(value) if value is not None else None
            else:
                encoded[key] = value
        return encoded

    def _decode_row(self, row: sqlite3.Row) -> dict[str, Any]:
        candidate = dict(row)
        candidate["factor_breakdown"] = json.loads(candidate["factor_breakdown"])
        candidate["tail_metrics"] = (
            json.loads(candidate["tail_metrics"])
            if candidate.get("tail_metrics")
            else None
        )
        return candidate


class OvernightTrackedTradeStore:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def initialize(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS overnight_tracked_trades (
                    trade_id TEXT PRIMARY KEY,
                    trade_date TEXT NOT NULL,
                    market_region TEXT NOT NULL DEFAULT 'cn_a',
                    scan_id TEXT NOT NULL,
                    scan_mode TEXT NOT NULL,
                    source_bucket TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    name TEXT NOT NULL,
                    pool TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    quick_score REAL NOT NULL,
                    total_score REAL NOT NULL,
                    factor_breakdown TEXT NOT NULL,
                    tail_metrics TEXT,
                    confirmed_at TEXT NOT NULL,
                    entry_target_time TEXT NOT NULL DEFAULT '14:55',
                    entry_price REAL,
                    entry_time_used TEXT,
                    exit_target_time TEXT NOT NULL DEFAULT '10:00',
                    exit_trade_date TEXT,
                    exit_price REAL,
                    exit_time_used TEXT,
                    strategy_return REAL,
                    status TEXT NOT NULL,
                    last_error TEXT,
                    last_checked_at TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(trade_date, market_region)
                )
                """
            )
            self._migrate_schema(conn)
            conn.commit()

    def create_trade(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = {
            "trade_id": payload["trade_id"],
            "trade_date": payload["trade_date"],
            "market_region": payload.get("market_region", "cn_a"),
            "scan_id": payload["scan_id"],
            "scan_mode": payload["scan_mode"],
            "source_bucket": payload["source_bucket"],
            "ticker": payload["ticker"],
            "name": payload["name"],
            "pool": payload["pool"],
            "quality": payload["quality"],
            "quick_score": payload["quick_score"],
            "total_score": payload["total_score"],
            "factor_breakdown": payload.get("factor_breakdown") or {},
            "tail_metrics": payload.get("tail_metrics"),
            "confirmed_at": payload["confirmed_at"],
            "entry_target_time": payload.get("entry_target_time", "14:55"),
            "entry_price": payload.get("entry_price"),
            "entry_time_used": payload.get("entry_time_used"),
            "exit_target_time": payload.get("exit_target_time", "10:00"),
            "exit_trade_date": payload.get("exit_trade_date"),
            "exit_price": payload.get("exit_price"),
            "exit_time_used": payload.get("exit_time_used"),
            "strategy_return": payload.get("strategy_return"),
            "status": payload["status"],
            "last_error": payload.get("last_error"),
            "last_checked_at": payload.get("last_checked_at"),
            "created_at": payload.get("created_at", utc_now()),
            "updated_at": payload.get("updated_at", utc_now()),
        }
        db_record = self._encode_record(record)
        placeholders = ", ".join("?" for _ in TRACKED_TRADE_COLUMNS)
        with closing(_connect(self.db_path)) as conn:
            try:
                conn.execute(
                    f"INSERT INTO overnight_tracked_trades ({', '.join(TRACKED_TRADE_COLUMNS)}) VALUES ({placeholders})",
                    [db_record[column] for column in TRACKED_TRADE_COLUMNS],
                )
            except sqlite3.IntegrityError as exc:
                raise ValueError("tracked_trade_already_exists") from exc
            conn.commit()
        return record

    def get_trade(self, trade_id: str) -> dict[str, Any] | None:
        with closing(_connect(self.db_path)) as conn:
            row = conn.execute(
                "SELECT * FROM overnight_tracked_trades WHERE trade_id = ?",
                (trade_id,),
            ).fetchone()
        return self._decode_row(row) if row else None

    def get_trade_by_date(
        self,
        trade_date: str,
        market_region: str = "cn_a",
    ) -> dict[str, Any] | None:
        with closing(_connect(self.db_path)) as conn:
            row = conn.execute(
                """
                SELECT * FROM overnight_tracked_trades
                WHERE trade_date = ? AND market_region = ?
                """,
                (trade_date, market_region),
            ).fetchone()
        return self._decode_row(row) if row else None

    def list_trades(self) -> list[dict[str, Any]]:
        with closing(_connect(self.db_path)) as conn:
            rows = conn.execute(
                """
                SELECT * FROM overnight_tracked_trades
                ORDER BY trade_date DESC, confirmed_at DESC
                """
            ).fetchall()
        return [self._decode_row(row) for row in rows]

    def list_refreshable(self) -> list[dict[str, Any]]:
        with closing(_connect(self.db_path)) as conn:
            rows = conn.execute(
                """
                SELECT * FROM overnight_tracked_trades
                WHERE status IN ('pending_entry', 'pending_exit', 'unavailable')
                ORDER BY trade_date ASC, confirmed_at ASC
                """
            ).fetchall()
        return [self._decode_row(row) for row in rows]

    def update_trade(self, trade_id: str, **fields: Any) -> dict[str, Any] | None:
        if not fields:
            return self.get_trade(trade_id)

        encoded = self._encode_record(fields)
        assignments = ", ".join(f"{column} = ?" for column in encoded.keys())
        values = list(encoded.values()) + [trade_id]
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                f"""
                UPDATE overnight_tracked_trades
                SET {assignments}
                WHERE trade_id = ?
                """,
                values,
            )
            conn.commit()
        return self.get_trade(trade_id)

    def delete_trade(self, trade_id: str) -> None:
        with closing(_connect(self.db_path)) as conn:
            conn.execute(
                "DELETE FROM overnight_tracked_trades WHERE trade_id = ?",
                (trade_id,),
            )
            conn.commit()

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        existing_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(overnight_tracked_trades)").fetchall()
        }
        migrations = {
            "market_region": "ALTER TABLE overnight_tracked_trades ADD COLUMN market_region TEXT NOT NULL DEFAULT 'cn_a'",
            "scan_mode": "ALTER TABLE overnight_tracked_trades ADD COLUMN scan_mode TEXT NOT NULL DEFAULT 'strict'",
            "source_bucket": "ALTER TABLE overnight_tracked_trades ADD COLUMN source_bucket TEXT NOT NULL DEFAULT 'formal'",
            "factor_breakdown": "ALTER TABLE overnight_tracked_trades ADD COLUMN factor_breakdown TEXT NOT NULL DEFAULT '{}'",
            "tail_metrics": "ALTER TABLE overnight_tracked_trades ADD COLUMN tail_metrics TEXT",
            "confirmed_at": "ALTER TABLE overnight_tracked_trades ADD COLUMN confirmed_at TEXT NOT NULL DEFAULT ''",
            "entry_target_time": "ALTER TABLE overnight_tracked_trades ADD COLUMN entry_target_time TEXT NOT NULL DEFAULT '14:55'",
            "entry_price": "ALTER TABLE overnight_tracked_trades ADD COLUMN entry_price REAL",
            "entry_time_used": "ALTER TABLE overnight_tracked_trades ADD COLUMN entry_time_used TEXT",
            "exit_target_time": "ALTER TABLE overnight_tracked_trades ADD COLUMN exit_target_time TEXT NOT NULL DEFAULT '10:00'",
            "exit_trade_date": "ALTER TABLE overnight_tracked_trades ADD COLUMN exit_trade_date TEXT",
            "exit_price": "ALTER TABLE overnight_tracked_trades ADD COLUMN exit_price REAL",
            "exit_time_used": "ALTER TABLE overnight_tracked_trades ADD COLUMN exit_time_used TEXT",
            "strategy_return": "ALTER TABLE overnight_tracked_trades ADD COLUMN strategy_return REAL",
            "status": "ALTER TABLE overnight_tracked_trades ADD COLUMN status TEXT NOT NULL DEFAULT 'pending_entry'",
            "last_error": "ALTER TABLE overnight_tracked_trades ADD COLUMN last_error TEXT",
            "last_checked_at": "ALTER TABLE overnight_tracked_trades ADD COLUMN last_checked_at TEXT",
            "created_at": "ALTER TABLE overnight_tracked_trades ADD COLUMN created_at TEXT NOT NULL DEFAULT ''",
            "updated_at": "ALTER TABLE overnight_tracked_trades ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''",
        }
        for column, statement in migrations.items():
            if column not in existing_columns:
                conn.execute(statement)

    def _encode_record(self, record: dict[str, Any]) -> dict[str, Any]:
        encoded: dict[str, Any] = {}
        for key, value in record.items():
            if key in {"factor_breakdown", "tail_metrics"}:
                encoded[key] = json.dumps(value) if value is not None else None
            else:
                encoded[key] = value
        return encoded

    def _decode_row(self, row: sqlite3.Row) -> dict[str, Any]:
        trade = dict(row)
        trade["factor_breakdown"] = json.loads(trade["factor_breakdown"])
        trade["tail_metrics"] = (
            json.loads(trade["tail_metrics"])
            if trade.get("tail_metrics")
            else None
        )
        return trade
