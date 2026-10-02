"""Reconcile lock, mint, burn and release records and flag duplicate, unmatched and delayed messages."""

import json


def analyze(data):
    now = data["timestamp"]
    timeout = data.get("timeout", 3600)
    if not isinstance(now, int) or not isinstance(timeout, int) or timeout < 0:
        raise ValueError("invalid time")
    messages = {}
    issues = []
    totals = {"lock": 0, "mint": 0, "burn": 0, "release": 0}
    event_ids = set()
    for event in data["events"]:
        kind = event["type"]
        amount = event["amount"]
        timestamp = event["timestamp"]
        eid = event["event_id"]
        if (
            kind not in totals
            or not isinstance(amount, int)
            or isinstance(amount, bool)
            or amount <= 0
            or timestamp > now
        ):
            raise ValueError("invalid bridge event")
        if eid in event_ids:
            issues.append({"kind": "duplicate_event", "event_id": eid})
            continue
        event_ids.add(eid)
        totals[kind] += amount
        key = (event["direction"], event["message_id"])
        bucket = messages.setdefault(key, {})
        if kind in bucket:
            issues.append(
                {"kind": "duplicate_message_stage", "message_id": key[1], "stage": kind}
            )
        else:
            bucket[kind] = event
    matched = 0
    pending = 0
    for (direction, mid), stages in messages.items():
        if direction == "outbound":
            source, target = "lock", "mint"
        elif direction == "inbound":
            source, target = "burn", "release"
        else:
            raise ValueError("invalid direction")
        unexpected = set(stages) - {source, target}
        if unexpected:
            issues.append(
                {
                    "kind": "unexpected_stage",
                    "message_id": mid,
                    "stages": sorted(unexpected),
                }
            )
        a = stages.get(source)
        b = stages.get(target)
        if b and not a:
            issues.append({"kind": "orphan_destination", "message_id": mid})
        elif a and not b:
            pending += 1
            if now - a["timestamp"] > timeout:
                issues.append({"kind": "overdue", "message_id": mid})
        elif a and b:
            mismatch = [k for k in ("amount", "asset", "recipient") if a[k] != b[k]]
            if mismatch:
                issues.append(
                    {"kind": "mismatch", "message_id": mid, "fields": mismatch}
                )
            elif b["timestamp"] < a["timestamp"]:
                issues.append({"kind": "time_inversion", "message_id": mid})
            else:
                matched += 1
    return {
        "totals": totals,
        "matched_messages": matched,
        "pending_messages": pending,
        "issues": issues,
        "locked_less_released": totals["lock"] - totals["release"],
        "minted_less_burned": totals["mint"] - totals["burn"],
    }


import argparse
import csv
import io
import sys
from pathlib import Path


def load_input(path):
    raw = Path(path).read_text(encoding="utf-8")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON key: " + key)
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("invalid JSON constant: " + value)

    data = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
    if not isinstance(data, dict):
        raise ValueError("input must be a JSON object")
    return data


def flatten(value, prefix=""):
    rows = []
    if isinstance(value, dict):
        for key, item in value.items():
            rows.extend(flatten(item, f"{prefix}.{key}" if prefix else key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            rows.extend(flatten(item, f"{prefix}[{index}]"))
    else:
        rows.append((prefix, json.dumps(value, ensure_ascii=False)))
    return rows


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", help="JSON input file")
    parser.add_argument("--output", help="write result atomically to a file")
    parser.add_argument("--format", choices=["json", "csv"], default="json")
    args = parser.parse_args(argv)
    try:
        result = analyze(load_input(args.input))
        if args.format == "json":
            content = (
                json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n"
            )
        else:
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(["path", "value"])
            writer.writerows(flatten(result))
            content = buffer.getvalue()
        if args.output:
            import os, tempfile

            target = Path(args.output)
            fd, temporary = tempfile.mkstemp(
                prefix="." + target.name + ".", dir=target.parent
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as stream:
                    stream.write(content)
                os.replace(temporary, target)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)
        else:
            sys.stdout.write(content)
    except (ValueError, KeyError, TypeError, OSError, ArithmeticError) as error:
        parser.exit(2, f"error: {error}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
