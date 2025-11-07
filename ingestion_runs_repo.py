# ingestion_runs_repo.py
from dataclasses import dataclass

DDL_INGESTION_RUNS = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  job_name VARCHAR(200) NOT NULL,
  started_at DATETIME NOT NULL,
  ended_at DATETIME NOT NULL,
  success_count INT NOT NULL DEFAULT 0,
  fail_count INT NOT NULL DEFAULT 0,
  note TEXT NULL
) COMMENT='数据同步运行记录';
"""

@dataclass
class IngestionRun:
    job_name: str
    started_at: object  # datetime
    ended_at: object    # datetime
    success_count: int = 0
    fail_count: int = 0
    note: str = None

class IngestionRunsRepo:
    def __init__(self, db_helper):
        self.db = db_helper

    def _ensure_connected(self):
        if not getattr(self.db, "cursor", None):
            self.db.connect()
        if not getattr(self.db, "cursor", None):
            raise RuntimeError("DB not connected; cannot operate ingestion_runs")

    def ensure_table(self):
        self._ensure_connected()
        self.db.cursor.execute(DDL_INGESTION_RUNS)
        self.db.conn.commit()

    def insert_run(self, run: IngestionRun):
        self._ensure_connected()
        sql = """
        INSERT INTO ingestion_runs
          (job_name, started_at, ended_at, success_count, fail_count, note)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.db.cursor.execute(sql, (
            run.job_name, run.started_at, run.ended_at,
            run.success_count, run.fail_count, run.note
        ))
        self.db.conn.commit()
