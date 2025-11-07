# ingestion_runs_repo.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from db_utils import DBHelper

DDL_INGESTION_RUNS = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  job_name VARCHAR(64) NOT NULL,
  started_at DATETIME NOT NULL,
  ended_at   DATETIME NOT NULL,
  success_count INT NOT NULL,
  fail_count    INT NOT NULL,
  note VARCHAR(255) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

@dataclass
class IngestionRun:
    job_name: str
    started_at: datetime
    ended_at: datetime
    success_count: int
    fail_count: int
    note: Optional[str] = None

class IngestionRunsRepo:
    """依赖现有 DBHelper（self.conn/self.cursor）。"""

    def __init__(self, db: DBHelper):
        self.db = db

    def ensure_table(self):
        self.db.cursor.execute(DDL_INGESTION_RUNS)
        self.db.conn.commit()

    def insert_run(self, run: IngestionRun) -> int:
        sql = """
        INSERT INTO ingestion_runs
          (job_name, started_at, ended_at, success_count, fail_count, note)
        VALUES
          (%s, %s, %s, %s, %s, %s)
        """
        self.db.cursor.execute(sql, (
            run.job_name,
            run.started_at,
            run.ended_at,
            run.success_count,
            run.fail_count,
            run.note,
        ))
        self.db.conn.commit()
        return self.db.cursor.lastrowid
