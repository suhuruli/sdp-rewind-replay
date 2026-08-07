#!/bin/bash
# Print the rewind command, with a real timestamp already filled in.
#
#   ./rewind-points.sh
#
# This does not call the rewind API. It prints the command for you to read and
# run yourself, because the API call is the thing worth seeing: hiding it inside
# a wrapper teaches nobody how to use the feature.
#
# It answers the one question the demo has to answer out loud, "rewind to WHEN?",
# and closes two traps on the way:
#
#   1. DESCRIBE HISTORY returns timestamps in the WAREHOUSE's local zone
#      (e.g. -07:00), while rewind_timestamp is interpreted as UTC. Pasting a
#      local timestamp straight out of history silently rewinds to the wrong
#      moment, potentially hours off, with no error at all.
#   2. The format is 'yyyy-MM-dd HH:mm:ss', space-separated. ISO-8601 is
#      rejected, and the error blames Delta log retention rather than the format.
#
# There is no public API for listing rewind points (the endpoint exists but is
# INTERNAL / workspace-UI only), so a timestamp is the only self-contained API
# workflow. Delta commit times are the best available proxy.
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-e2-dogfood}"
export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
CATALOG="${CATALOG:-harsha_rewind_demo}"
SCHEMA="${SCHEMA:-payments}"
PIPELINE_ID="${PIPELINE_ID:-87ba1cf1-9a9c-4750-8ad0-92690f61ae2c}"
# Must be serverless or pro. A 2X-Small classic warehouse cannot parse
# DESCRIBE HISTORY and fails with TABLE_OR_VIEW_NOT_FOUND: HISTORY.
WAREHOUSE_ID="${WAREHOUSE_ID:-864004c1b3961382}"

# to_utc_timestamp(ts, current_timezone()) reinterprets the session-local commit
# time as UTC, which is the frame rewind_timestamp is read in. The SQL statement
# API takes a single statement, so SET TIME ZONE is not available here.
SQL="SELECT version,
       date_format(to_utc_timestamp(timestamp, current_timezone()),
                   'yyyy-MM-dd HH:mm:ss') AS ts_utc,
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

echo "silver_payments commit history, timestamps converted to UTC:"
echo

databricks api post /api/2.0/sql/statements -p "$PROFILE" --json "$body" \
  | CATALOG="$CATALOG" SCHEMA="$SCHEMA" PIPELINE_ID="$PIPELINE_ID" \
    PROFILE="$PROFILE" python3 -c '
import json, os, sys

d = json.load(sys.stdin)
if d.get("status", {}).get("state") != "SUCCEEDED":
    print("QUERY FAILED:", json.dumps(d.get("status", {}))[:800])
    sys.exit(1)

rows = d.get("result", {}).get("data_array", []) or []
print("%4s  %-21s  %-18s  %s" % ("ver", "timestamp (UTC)", "operation", "out_rows"))
print("-" * 62)
for v, ts, op, out in rows:
    print("%4s  %-21s  %-18s  %s" % (v, ts, op or "", out or ""))

# Suggest the newest STREAMING UPDATE. In the demo that is the corrupt one, and
# rewinding to its commit time lands just before it wrote.
updates = [r for r in rows if (r[2] or "") == "STREAMING UPDATE"]
if not updates:
    print("\nNo STREAMING UPDATE found. Has the pipeline run yet?")
    sys.exit(0)

ts = updates[0][1]
cat, sch = os.environ["CATALOG"], os.environ["SCHEMA"]
pid, prof = os.environ["PIPELINE_ID"], os.environ["PROFILE"]

print()
print("Newest update is version %s at %s UTC." % (updates[0][0], ts))
print("Rewind to just before it. Both silver AND gold must be named: downstream")
print("cascade is documented but does not currently happen, and leaving gold")
print("un-rewound hard-blocks the pipeline on the next update.")
print()
print("  databricks pipelines start-update %s \\" % pid)
print("    -p %s --json '"'"'{" % prof)
print("    \"cause\": \"API_CALL\",")
print("    \"rewind_spec\": {")
print("      \"rewind_timestamp\": \"%s\"," % ts)
print("      \"datasets\": [")
print("        { \"identifier\": \"%s.%s.silver_payments\" }," % (cat, sch))
print("        { \"identifier\": \"%s.%s.gold_merchant_5min\" }" % (cat, sch))
print("      ]")
print("    }")
print("  }'"'"'")
print()
print("Then replay. Note it is an ordinary update, with no special flags:")
print()
print("  databricks pipelines start-update %s -p %s" % (pid, prof))
print()
print("Fix the code BEFORE replaying. Rewind restores data, never code.")
'
