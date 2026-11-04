"""Compare gateway-exported CSV events with the local event ledger."""

import argparse
import csv
import sqlite3
import sys


def reconcile(database_path: str, csv_path: str) -> int:
    """Print gateway events absent locally or whose important fields disagree."""
    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    problems = 0
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        for gateway_event in csv.DictReader(csv_file):
            local = connection.execute(
                "SELECT event_id, payload, status FROM events WHERE event_id = ?", (gateway_event["event_id"],)
            ).fetchone()
            if local is None:
                print(f"MISSING: {gateway_event['event_id']}")
                problems += 1
                continue
            # CSV records are strings, so compare string versions of values.
            import json
            payload = json.loads(local["payload"])
            for field in ("type", "amount", "order_id"):
                if field in gateway_event and str(payload.get(field)) != gateway_event[field]:
                    print(f"MISMATCH: {gateway_event['event_id']} field {field}")
                    problems += 1
                    break
    connection.close()
    if not problems:
        print("Reconciliation passed: no missing or mismatched gateway events.")
    return problems


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path")
    parser.add_argument("--database", default="data/webhooks.db")
    arguments = parser.parse_args()
    sys.exit(1 if reconcile(arguments.database, arguments.csv_path) else 0)
