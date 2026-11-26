import tempfile, os, time, unittest
from orchestrator import Orchestrator

class OrchestratorTests(unittest.TestCase):
    def setUp(self):
        handle = tempfile.NamedTemporaryFile(delete=False)
        self.path = handle.name
        handle.close()
        self.app = Orchestrator(self.path, worker_count=2, demo_delay=0.25)
    def tearDown(self):
        self.app.stop()
        os.unlink(self.path)
    def wait_for(self, task_ids, status="completed", timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline:
            rows = {task["id"]: task for task in self.app.list_tasks()}
            if all(rows.get(task_id, {}).get("status") == status for task_id in task_ids):
                return rows
            time.sleep(.03)
        return {task["id"]: task for task in self.app.list_tasks()}
    def test_conflicting_shared_source_serialized(self):
        a = self.app.submit("A", "2026-11-20", ["shared"])
        b = self.app.submit("B", "2026-11-21", ["shared"])
        self.app.start()
        rows = self.wait_for([a, b])
        self.assertEqual(rows[a]["status"], "completed")
        self.assertEqual(rows[b]["status"], "completed")
        self.assertLessEqual(rows[a]["started_at"], rows[b]["started_at"])
    def test_independent_tasks_run_in_parallel(self):
        a = self.app.submit("A", "2026-11-20", ["source_a"])
        b = self.app.submit("B", "2026-11-20", ["source_b"])
        self.app.start()
        deadline = time.time() + 3
        while time.time() < deadline:
            rows = {task["id"]: task for task in self.app.list_tasks()}
            if rows[a]["status"] == rows[b]["status"] == "running": break
            time.sleep(.02)
        self.assertEqual(rows[a]["status"], "running")
        self.assertEqual(rows[b]["status"], "running")
    def test_duplicate_request_is_idempotent(self):
        a = self.app.submit("A", "2026-11-20", ["source", "source"])
        b = self.app.submit("A", "2026-11-20", ["source"])
        self.assertEqual(a, b)
        self.assertEqual(len(self.app.list_tasks()), 1)
    def test_same_process_and_date_conflict(self):
        a = self.app.submit("A", "2026-11-20", ["source_a"])
        b = self.app.submit("A", "2026-11-20", ["source_b"])
        self.app.start()
        rows = self.wait_for([a, b])
        self.assertEqual(rows[a]["status"], "completed")
        self.assertEqual(rows[b]["status"], "completed")
        self.assertLessEqual(rows[a]["started_at"], rows[b]["started_at"])
    def test_invalid_date_rejected(self):
        with self.assertRaises(ValueError):
            self.app.submit("A", "20-11-2026", ["source"])

if __name__ == "__main__":
    unittest.main()
