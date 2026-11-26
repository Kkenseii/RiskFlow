"""Учебный оркестратор RiskFlow на SQLite и потоках Python."""
import json
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).with_name("riskflow.db")
SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY, process TEXT NOT NULL, business_date TEXT NOT NULL,
 source_keys TEXT NOT NULL, requested_by TEXT NOT NULL, trigger TEXT NOT NULL,
 status TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT,
 finished_at TEXT, message TEXT, request_key TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS resource_locks (
 resource_key TEXT PRIMARY KEY, task_id TEXT NOT NULL, acquired_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS task_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL,
 event TEXT NOT NULL, created_at TEXT NOT NULL, message TEXT
);
"""

class Orchestrator:
    """Атомарно захватывает ресурсы задачи; SQLite подходит для учебного MVP на одном узле."""
    def __init__(self, db_path=DB_PATH, worker_count=3, demo_delay=2, lock_timeout=300):
        self.db_path = str(db_path)
        self.worker_count = max(1, int(worker_count))
        self.demo_delay = max(0, float(demo_delay))
        self.lock_timeout = max(1, int(lock_timeout))
        self._stop = threading.Event()
        self._dispatcher = None
        self._workers = set()
        self._init_db()
        self._recover_stale()

    def _connect(self):
        con = sqlite3.connect(self.db_path, timeout=10)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA busy_timeout=10000")
        return con

    def _init_db(self):
        with self._connect() as con:
            con.executescript(SCHEMA)
            # Миграция для существующей базы MVP.
            columns = {row[1] for row in con.execute("PRAGMA table_info(tasks)")}
            if "request_key" not in columns:
                con.execute("ALTER TABLE tasks ADD COLUMN request_key TEXT")
            con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_request_key ON tasks(request_key)")
            lock_columns = {row[1] for row in con.execute("PRAGMA table_info(resource_locks)")}
            if "acquired_at" not in lock_columns:
                con.execute("ALTER TABLE resource_locks ADD COLUMN acquired_at TEXT")

    @staticmethod
    def _event(con, task_id, event, message=""):
        con.execute("INSERT INTO task_events(task_id,event,created_at,message) VALUES (?,?,?,?)",
                    (task_id, event, datetime.now().isoformat(timespec="seconds"), message))

    def _recover_stale(self):
        """При старте освобождает блокировки старше TTL и помечает задачу ошибочной."""
        cutoff = (datetime.now() - timedelta(seconds=self.lock_timeout)).isoformat(timespec="seconds")
        with self._connect() as con:
            stale = con.execute("SELECT DISTINCT task_id FROM resource_locks WHERE acquired_at IS NULL OR acquired_at < ?", (cutoff,)).fetchall()
            for row in stale:
                task_id = row["task_id"]
                con.execute("DELETE FROM resource_locks WHERE task_id=?", (task_id,))
                con.execute("UPDATE tasks SET status='failed',finished_at=?,message=? WHERE id=? AND status='running'",
                            (datetime.now().isoformat(timespec="seconds"), "Блокировка освобождена после тайм-аута", task_id))
                self._event(con, task_id, "recovered", "Блокировка освобождена после тайм-аута")

    def submit(self, process, business_date, source_keys, requested_by="demo", trigger="manual"):
        process = str(process).strip()
        if not process:
            raise ValueError("Укажите бизнес-процесс.")
        try:
            datetime.strptime(str(business_date), "%Y-%m-%d")
        except ValueError:
            raise ValueError("Дата должна быть в формате ГГГГ-ММ-ДД.")
        source_keys = sorted(set(str(s).strip() for s in source_keys if str(s).strip()))
        if not source_keys:
            raise ValueError("Укажите хотя бы один источник данных.")
        if trigger not in ("manual", "scheduled"):
            raise ValueError("trigger должен быть manual или scheduled")
        # Одинаковый процесс + дата + набор источников = один запуск, независимо от инициатора.
        request_key = json.dumps([process, str(business_date), source_keys], ensure_ascii=False, separators=(",", ":"))
        task_id = str(uuid.uuid4())[:8]
        now = datetime.now().isoformat(timespec="seconds")
        with self._connect() as con:
            try:
                con.execute("""INSERT INTO tasks
                    (id,process,business_date,source_keys,requested_by,trigger,status,created_at,request_key)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (task_id, process, str(business_date), json.dumps(source_keys, ensure_ascii=False),
                     requested_by, trigger, "queued", now, request_key))
                self._event(con, task_id, "queued", f"Запуск: {trigger}")
            except sqlite3.IntegrityError:
                existing = con.execute("SELECT id FROM tasks WHERE request_key=?", (request_key,)).fetchone()
                if existing:
                    return existing["id"]
                raise
        return task_id

    def list_tasks(self):
        with self._connect() as con:
            rows = con.execute("SELECT * FROM tasks ORDER BY created_at DESC, rowid DESC").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["source_keys"] = json.loads(item["source_keys"])
            result.append(item)
        return result

    def list_locks(self):
        with self._connect() as con:
            return [dict(row) for row in con.execute("SELECT * FROM resource_locks ORDER BY resource_key")]

    def _resources(self, row):
        # Процесс+дата защищает одинаковый расчёт; источник защищает общие данные между датами/процессами.
        resources = [f"process:{row['process']}:{row['business_date']}"]
        resources.extend(f"source:{key}" for key in json.loads(row["source_keys"]))
        return sorted(set(resources))

    def _claim(self, task_id):
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute("SELECT * FROM tasks WHERE id=? AND status='queued'", (task_id,)).fetchone()
            if row is None:
                con.rollback(); return False
            resources = self._resources(row)
            marks = ",".join("?" for _ in resources)
            if con.execute(f"SELECT 1 FROM resource_locks WHERE resource_key IN ({marks}) LIMIT 1", resources).fetchone():
                con.rollback(); return False
            now = datetime.now().isoformat(timespec="seconds")
            con.executemany("INSERT INTO resource_locks(resource_key,task_id,acquired_at) VALUES (?,?,?)",
                            [(key, task_id, now) for key in resources])
            con.execute("UPDATE tasks SET status='running',started_at=?,message=? WHERE id=?", (now, "Ресурсы захвачены", task_id))
            self._event(con, task_id, "started", "Ресурсы захвачены")
            con.commit()
            return True
        except sqlite3.Error:
            con.rollback(); return False
        finally:
            con.close()

    def _finish(self, task_id, status="completed", message="Успешно завершено"):
        now = datetime.now().isoformat(timespec="seconds")
        with self._connect() as con:
            con.execute("DELETE FROM resource_locks WHERE task_id=?", (task_id,))
            con.execute("UPDATE tasks SET status=?,finished_at=?,message=? WHERE id=?", (status, now, message, task_id))
            self._event(con, task_id, status, message)

    def _run_task(self, task):
        try:
            time.sleep(self.demo_delay)  # Здесь подключается реальный алгоритм оценки риска.
            self._finish(task["id"], "completed", "Демо-расчет завершен")
        except Exception as exc:
            self._finish(task["id"], "failed", str(exc))

    def start(self):
        if self._dispatcher and self._dispatcher.is_alive():
            return
        self._stop.clear()
        self._dispatcher = threading.Thread(target=self._dispatch, daemon=True)
        self._dispatcher.start()

    def _dispatch(self):
        while not self._stop.is_set():
            self._workers = {worker for worker in self._workers if worker.is_alive()}
            free = self.worker_count - len(self._workers)
            if free > 0:
                for task in self.list_tasks():
                    if free <= 0: break
                    if task["status"] != "queued": continue
                    if self._claim(task["id"]):
                        worker = threading.Thread(target=self._run_task, args=(task,), daemon=True)
                        self._workers.add(worker); worker.start(); free -= 1
            time.sleep(0.2)

    def stop(self):
        self._stop.set()
        if self._dispatcher:
            self._dispatcher.join(timeout=2)
        for worker in list(self._workers):
            worker.join(timeout=max(2, self.demo_delay + 1))

if __name__ == "__main__":
    engine = Orchestrator()
    for process, date, sources, user, trigger in [
        ("Кредитный риск", "2026-11-20", ["warehouse_credit"], "Иван", "manual"),
        ("Кредитный риск", "2026-11-21", ["warehouse_credit"], "Анна", "scheduled"),
        ("Рыночный риск", "2026-11-20", ["market_data"], "Пётр", "manual")]:
        engine.submit(process, date, sources, user, trigger)
    engine.start()
    print("Оркестратор запущен. Ctrl+C для остановки.")
    try:
        while True:
            for task in engine.list_tasks(): print(task["id"], task["process"], task["business_date"], task["status"], task["message"])
            time.sleep(2)
    except KeyboardInterrupt:
        engine.stop()
