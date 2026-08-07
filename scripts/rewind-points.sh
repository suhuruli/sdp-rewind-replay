#!/bin/bash
# Print candidate rewind timestamps, already formatted and already in UTC.
#
#   ./rewind-points.sh
#
# Answers the one question the demo has to answer out loud: "rewind to WHEN?"
#
# Two traps this exists to close:
#
#   1. DESCRIBE HISTORY returns timestamps in the WAREHOUSE's local zone
#      (e.g. -07:00). rewind_timestamp is interpreted as UTC. Pasting a local
#      timestamp straight from history silently rewinds to the wrong moment,
#      possibly hours off, with no error.
#   2. The required format is 'yyyy-MM-dd HH:mm:ss', space-separated. ISO-8601
#      is rejected and the error blames Delta log retention instead.
#
# There is no public API to list rewind points (the endpoint exists but is
# INTERNAL / workspace-UI only), so timestamp-based rewind is the only
# self-contained API workflow. Delta commit times are the best proxy: pick a
# timestamp just BEFORE the first bad update.
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-e2-dogfood}"
export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
CATALOG="${CATALOG:-harsha_rewind_demo}"
SCHEMA="${SCHEMA:-payments}"
WAREHOUSE_ID="${WAREHOUSE_ID:-864004c1b3961382}"

# The SQL statement API takes ONE statement, so SET TIME ZONE is not available.
# to_utc_timestamp(ts, current_timezone()) reinterprets the session-local commit
# time as UTC, which is what rewind_timestamp expects.
SQL="SELECT version,
       date_format(to_utc_timestamp(timestamp, current_timezone()),
                   'yyyy-MM-dd HH:mm:ss') AS rewind_timestamp_utc,
       operation,
       operationMetrics['numOutputRows'] AS out_rows
FROM (DESCRIBE HISTORY $CATALOG.$SCHEMA.silver_payments)
ORDER BY version DESC LIMIT 15"

body=$(python3 -c '
import json, sys
print(json.dumps({
    "warehouse_id": sys.argv[1],
    "statement": sys.argv[2],
    "format": "JSON_ARRAY",
    "disposition": "INLINE",
    "wait_timeout": "50s",
}))' "$WAREHOUSE_ID" "$SQL")

echo "silver_payments commit history, timestamps in UTC and ready to paste:"
echo

databricks api post /api/2.0/sql/statements -p "$PROFILE" --json "$body" | python3 -c '
import json, sys
d = json.load(sys.stdin)
if d.get("status", {}).get("state") != "SUCCEEDED":
    print("QUERY FAILED:", json.dumps(d.get("status", {}))[:800]); sys.exit(1)
rows = d.get("result", {}).get("data_array", []) or []
print("%4s  %-21s  %-18s  %s" % ("ver", "rewind_timestamp (UTC)", "operation", "out_rows"))
print("-" * 62)
for v, ts, op, out in rows:
    print("%4s  %-21s  %-18s  %s" % (v, ts, op or "", out or ""))
print()
print("Pass a timestamp from just BEFORE the first bad update:")
print("  ./scripts/rewind.sh --replay <rewind_timestamp>")
'
