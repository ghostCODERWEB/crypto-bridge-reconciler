import json
import unittest
from cryptolab import *

EXAMPLE = json.loads(
    '{"timestamp": 1000, "events": [{"event_id": "a", "message_id": "1", "direction": "outbound", "type": "lock", "amount": 100, "asset": "USD", "recipient": "alice", "timestamp": 100}, {"event_id": "b", "message_id": "1", "direction": "outbound", "type": "mint", "amount": 100, "asset": "USD", "recipient": "alice", "timestamp": 200}]}'
)


class EngineTests(unittest.TestCase):
    def test_match(self):
        r = analyze(EXAMPLE)
        self.assertEqual(r["matched_messages"], 1)
        self.assertEqual(r["issues"], [])

    def test_mismatch(self):
        d = {
            **EXAMPLE,
            "events": [EXAMPLE["events"][0], {**EXAMPLE["events"][1], "amount": 101}],
        }
        self.assertEqual(analyze(d)["issues"][0]["kind"], "mismatch")

    def test_duplicates(self):
        d = {**EXAMPLE, "events": EXAMPLE["events"] + [EXAMPLE["events"][0]]}
        r = analyze(d)
        self.assertEqual(r["totals"]["lock"], 100)
        self.assertEqual(r["issues"][0]["kind"], "duplicate_event")

    def test_deterministic(self):
        self.assertEqual(analyze(EXAMPLE), analyze(EXAMPLE))

    def test_cli_json(self):
        import subprocess, sys

        result = subprocess.run(
            [sys.executable, "-m", "cryptolab", "examples/input.json"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), analyze(EXAMPLE))

    def test_cli_csv(self):
        import subprocess, sys, csv, io

        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "cryptolab",
                "examples/input.json",
                "--format",
                "csv",
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            list(csv.reader(io.StringIO(result.stdout)))[0], ["path", "value"]
        )

    def test_duplicate_json_keys(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text('{"a":1,"a":2}')
            with self.assertRaises(ValueError):
                load_input(path)

    def test_atomic_output(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "out.json"
            self.assertEqual(main(["examples/input.json", "--output", str(path)]), 0)
            self.assertEqual(json.loads(path.read_text()), analyze(EXAMPLE))

    def test_input_unchanged(self):
        import copy

        before = copy.deepcopy(EXAMPLE)
        analyze(EXAMPLE)
        self.assertEqual(before, EXAMPLE)


if __name__ == "__main__":
    unittest.main()
