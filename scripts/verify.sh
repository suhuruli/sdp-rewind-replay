#!/bin/bash
# Show the state of the three pipeline tables: row counts, Delta history, gold
# windows, and the correctness checks that prove a replay was clean.
#
# Rewinds emit no rewind-specific events, so looking for RESTORE in DESCRIBE
# HISTORY is the only way to confirm what a rewind actually touched. Run this
# before and after to diff.
#
#   ./verify.sh
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-e2-dogfood}"
export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
CATALOG="${CATALOG:-harsha_rewind_demo}"
SCHEMA="${SCHEMA:-payments}"
# Must be serverless or pro. A 2X-Small classic warehouse cannot parse
# DESCRIBE HISTORY and fails with TABLE_OR_VIEW_NOT_FOUND: HISTORY.
WAREHOUSE_ID="${WAREHOUSE_ID:-864004c1b3961382}"

run_sql() {
  local body
  body=$(python3 -c '
import json, sys
print(json.dumps({
    "warehouse_id": sys.argv[1],
    "statement": sys.argv[2],
    "format": "JSON_ARRAY",
    "disposition": "INLINE",
    "wait_timeout": "50s",
}))' "$WAREHOUSE_ID" "$1")

  databricks api post /api/2.0/sql/statements -p "$PROFILE" --json "$body" 2>&1 | python3 -c '
import json, sys
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception:
    print(raw[:1500]); sys.exit(1)
if d.get("status", {}).get("state") != "SUCCEEDED":
    print("QUERY FAILED:", json.dumps(d.get("status", {}))[:800]); sys.exit(1)
cols = [c["name"] for c in d["manifest"]["schema"]["columns"]]
rows = d.get("result", {}).get("data_array", []) or []
w = [len(c) for c in cols]
for r in rows:
    for i, v in enumerate(r):
        w[i] = max(w[i], len("NULL" if v is None else str(v)))
print("  ".join(c.ljust(w[i]) for i, c in enumerate(cols)))
print("  ".join("-" * x for x in w))
for r in rows:
    print("  ".join(("NULL" if v is None else str(v)).ljust(w[i]) for i, v in enumerate(r)))
'
}

echo "=================== ROW COUNTS ==================="
run_sql "SELECT 'landing' AS layer, count(*) AS rows FROM $CATALOG.$SCHEMA.landing_payments
UNION ALL SELECT 'bronze', count(*) FROM $CATALOG.$SCHEMA.bronze_payments
UNION ALL SELECT 'silver', count(*) FROM $CATALOG.$SCHEMA.silver_payments
UNION ALL SELECT 'gold',   count(*) FROM $CATALOG.$SCHEMA.gold_merchant_5min"

echo
echo "============ DELTA HISTORY (look for RESTORE) ============"
for t in bronze_payments silver_payments gold_merchant_5min; do
  run_sql "SELECT '$t' AS tbl, version, timestamp, operation,
                  operationMetrics['numOutputRows'] AS out_rows
           FROM (DESCRIBE HISTORY $CATALOG.$SCHEMA.$t)
           ORDER BY version DESC LIMIT 4"
  echo
done

echo "=================== GOLD WINDOWS ==================="
# avg_ticket is the diagnostic: a 100x valuation bug inflates dollars while
# leaving txns untouched, so this column moves and txns does not.
run_sql "SELECT window_start,
                sum(txn_count) AS txns,
                round(sum(total_amount), 2) AS settled,
                round(sum(total_amount) / sum(txn_count), 2) AS avg_ticket
         FROM $CATALOG.$SCHEMA.gold_merchant_5min
         GROUP BY window_start ORDER BY window_start DESC LIMIT 12"

echo
echo "=================== CORRECTNESS ==================="
# All three must read 0 after a clean replay. Duplicate windows are the signature
# of offsets moving without operator state; duplicate ids mean silver re-ingested.
run_sql "SELECT
  (SELECT count(*) FROM (SELECT payment_id FROM $CATALOG.$SCHEMA.silver_payments
     GROUP BY payment_id HAVING count(*) > 1)) AS dupe_payment_ids,
  (SELECT count(*) FROM (SELECT window_start, merchant_id FROM $CATALOG.$SCHEMA.gold_merchant_5min
     GROUP BY window_start, merchant_id HAVING count(*) > 1)) AS dupe_windows,
  (SELECT count(*) FROM $CATALOG.$SCHEMA.gold_merchant_5min
     WHERE avg_ticket > 1000) AS windows_still_inflated"
