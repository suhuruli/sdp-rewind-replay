#!/bin/bash
# Rewind the demo pipeline to a timestamp, then optionally replay.
#
#   ./rewind.sh --dry-run  "2026-08-06 17:37:00"
#   ./rewind.sh            "2026-08-06 17:37:00"
#   ./rewind.sh --replay   "2026-08-06 17:37:00"
#
# TIMESTAMP FORMAT: 'yyyy-MM-dd HH:mm:ss' in UTC, space-separated. ISO-8601 with
# 'T' and 'Z' is REJECTED, and the resulting error misleadingly blames Delta log
# retention rather than the format. This is the single most common way to waste an
# hour on this feature, so the format is validated below before any API call.
#
# The rewind names silver AND gold explicitly rather than relying on automatic
# downstream cascade. Cascade is documented but does not currently happen: naming
# only silver leaves gold un-rewound, and the next update then fails fatally with
# DELTA_SOURCE_IGNORE_DELETE, hard-blocking the pipeline until a full refresh.
# Bronze is deliberately left out, so the source is never re-read.
set -euo pipefail

PROFILE="${DATABRICKS_PROFILE:-e2-dogfood}"
export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
CATALOG="${CATALOG:-harsha_rewind_demo}"
SCHEMA="${SCHEMA:-payments}"

DRY_RUN=false
REPLAY=false
while [[ "${1:-}" == --* ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    --replay)  REPLAY=true;  shift ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

TS="${1:-}"
if [[ -z "$TS" ]]; then
  echo "usage: $0 [--dry-run] [--replay] 'yyyy-MM-dd HH:mm:ss'" >&2
  exit 1
fi

if [[ ! "$TS" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}\ [0-9]{2}:[0-9]{2}:[0-9]{2}$ ]]; then
  echo "ERROR: timestamp must be 'yyyy-MM-dd HH:mm:ss' (space-separated, UTC)." >&2
  echo "       Got: '$TS'" >&2
  echo "       ISO-8601 is rejected by the API and reports a misleading" >&2
  echo "       BEYOND_RETENTION error blaming Delta log cleanup." >&2
  exit 1
fi

PIPELINE_ID="${PIPELINE_ID:-$(databricks pipelines list-pipelines -p "$PROFILE" -o json 2>/dev/null \
  | python3 -c "
import sys, json
for p in json.load(sys.stdin):
    if 'sdp-rewind-replay-payments' in (p.get('name') or ''):
        print(p['pipeline_id']); break
")}"

if [[ -z "$PIPELINE_ID" ]]; then
  echo "ERROR: could not find pipeline 'sdp-rewind-replay-payments'." >&2
  echo "       Deploy the bundle first, or set PIPELINE_ID explicitly." >&2
  exit 1
fi

echo "pipeline : $PIPELINE_ID"
echo "target   : $TS UTC"
echo "dry run  : $DRY_RUN"

wait_for_update() {
  local label="$1"
  while true; do
    local state
    state=$(databricks pipelines get "$PIPELINE_ID" -p "$PROFILE" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
print((d.get('latest_updates') or [{}])[0].get('state', 'UNKNOWN'))
")
    printf '\r  %s: %-24s' "$label" "$state"
    case "$state" in
      COMPLETED) echo; return 0 ;;
      FAILED|CANCELED) echo; return 1 ;;
    esac
    sleep 15
  done
}

echo
echo "--- rewinding silver_payments + gold_merchant_5min ---"
databricks pipelines start-update "$PIPELINE_ID" -p "$PROFILE" --json "$(cat <<JSON
{
  "cause": "API_CALL",
  "rewind_spec": {
    "rewind_timestamp": "$TS",
    "dry_run": $DRY_RUN,
    "datasets": [
      { "identifier": "$CATALOG.$SCHEMA.silver_payments" },
      { "identifier": "$CATALOG.$SCHEMA.gold_merchant_5min" }
    ]
  }
}
JSON
)" > /dev/null

if ! wait_for_update "rewind"; then
  echo "rewind FAILED. Note that a failed rewind is NOT a no-op: it may already" >&2
  echo "have written RESTORE commits. Run scripts/verify.sh and check Delta" >&2
  echo "history before retrying." >&2
  exit 1
fi

if [[ "$DRY_RUN" == true ]]; then
  echo
  echo "Dry run passed. Caveat: dry run does not validate everything a real rewind"
  echo "checks, so a pass is not a guarantee. It also returns no resolved rewind"
  echo "point, so there is nothing further to inspect."
  exit 0
fi

echo
echo "Rewind complete. Verify with scripts/verify.sh — rewinds emit no"
echo "rewind-specific events, so DESCRIBE HISTORY looking for RESTORE is the only"
echo "way to confirm what actually moved."

if [[ "$REPLAY" == true ]]; then
  echo
  echo "--- replaying (ordinary pipeline update) ---"
  echo "Reminder: rewind does not restore code. Deploy your fix before replaying."
  databricks pipelines start-update "$PIPELINE_ID" --cause API_CALL -p "$PROFILE" > /dev/null
  wait_for_update "replay" || { echo "replay FAILED" >&2; exit 1; }
  echo "Replay complete."
fi
