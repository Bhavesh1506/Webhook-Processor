"""Compare gateway-exported CSV events with the local event ledger."""

import argparse
import csv
import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path


def reconcile(database_path: str, csv_path: str) -> int:
    """Print gateway events absent locally or whose important fields disagree."""
    problems = 0
    uri = Path(database_path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as connection, open(csv_path, newline="", encoding="utf-8-sig") as csv_file:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN")
        rows = csv.DictReader(csv_file, strict=True)
        fields = ("event_id", "type", "amount", "order_id")
        if rows.fieldnames is None or not set(fields).issubset(rows.fieldnames):
            raise ValueError("CSV must contain event_id, type, amount and order_id columns")
        if len(rows.fieldnames) != len(set(rows.fieldnames)):
            raise ValueError("CSV contains duplicate column names")
        for gateway_event in rows:
            if None in gateway_event or any(not gateway_event.get(field) for field in fields):
                raise ValueError(f"invalid CSV row ending on line {rows.line_num}")
            local = connection.execute(
                "SELECT event_id, payload, status FROM events WHERE event_id = ?", (gateway_event["event_id"],)
            ).fetchone()
            if local is None:
                print(f"MISSING: {gateway_event['event_id']}")
                problems += 1
                continue
            # CSV records are strings, so compare string versions of values.
            payload = json.loads(local["payload"])
            if not isinstance(payload, dict):
                raise ValueError("database contains an invalid event payload")
            for field in fields:
                if field != "event_id" and str(payload.get(field)) != gateway_event[field]:
                    print(f"MISMATCH: {gateway_event['event_id']} field {field}")
                    problems += 1
                    break
    if not problems:
        print("Reconciliation passed: no missing or mismatched gateway events.")
    return problems


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path")
    parser.add_argument("--database", default="data/webhooks.db")
    arguments = parser.parse_args()
    try:
        sys.exit(1 if reconcile(arguments.database, arguments.csv_path) else 0)
    except (OSError, sqlite3.Error, csv.Error, ValueError) as error:
        parser.exit(2, f"Reconciliation error: {error}\n")
