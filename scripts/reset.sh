#!/bin/bash
# Rebuild the demo's opening state: a contiguous healthy history, then a corrupt
# tail that is already visible on the dashboard.
#
#   ./reset.sh
#
# Leaves the repo and the workspace ready to record. Nothing during the recording
# stages an event; every artifact the demo points at is already real.
#
#   1. restore good code, deploy
#   2. empty landing, seed a contiguous healthy tail
#   3. FULL REFRESH -> the tail materializes healthy, and checkpoints plus
#      operator state are reset to match
#   4. apply the bug, deploy
#   5. seed the corrupt batch, plus a later batch to advance the watermark
#   6. incremental update -> only the new rows go through the broken transform
#
# The bug is an UNCOMMITTED edit to src/pipeline.py, so nothing here touches git
# history. On camera it is a 'git diff': one line, one color, no history to
# navigate. The fix is 'git checkout -- src/pipeline.py'. Committing the bug and
# reverting it reads as slightly more realistic, but it means every rehearsal
# leaves another commit behind and the script has to rewrite history to clean up.
# Not worth the moving parts in a live demo.
#
# WHY EVERYTHING IS CLEARED. Reusing what is already in landing is cheaper, but
# event time then has a multi-hour hole between the last rehearsal and this one,
# and rows landing behind an already-advanced watermark are dropped by the
# aggregation entirely: the bug stays real in silver and shows up nowhere. A
# fresh contiguous window makes every rehearsal identical and gives the clean
# "long flat stretch, then a cliff" shape the demo is built around.
#
# WHY THE CORRUPT TAIL NEEDS TWO BATCHES. Gold emits a window only once the
# watermark passes the window's end, and the watermark trails the largest event
# time seen by 10 minutes. A batch spanning S minutes therefore leaves all but
# its last S-10 minutes sitting in open windows: a single batch cannot close its
# own windows. The second batch, seeded at a much smaller lag, drags the
# watermark forward and closes the corrupt windows so the spike is visible. Skip
# it and the corruption is real, is sitting in silver, and shows up nowhere.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

PROFILE="${DATABRICKS_PROFILE:-e2-dogfood}"
export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
PIPELINE_ID="${PIPELINE_ID:-87ba1cf1-9a9c-4750-8ad0-92690f61ae2c}"
SEED_JOB_ID="${SEED_JOB_ID:-972844999617450}"
WAREHOUSE_ID="${WAREHOUSE_ID:-864004c1b3961382}"
CATALOG="${CATALOG:-harsha_rewind_demo}"
SCHEMA="${SCHEMA:-payments}"
BOUNDARY_FILE="$REPO/.demo-boundary"

# Healthy tail: two batches spreading 60 minutes of event time each, at
# descending lag so they abut. A contiguous 2 hours ending 90 minutes ago, which
# is 24 five-minute windows across 6 merchants. Enough for the chart to read as a
# long flat baseline; more events only make the rebuild slower.
HEALTHY_BATCHES=("210 60 300" "150 60 300")   # lag spread events

# Corrupt batch, then the watermark advancer. The advancer's own windows stay
# open, so it adds no second spike.
CORRUPT_BATCH="60 25 250"
ADVANCE_BATCH="10 5 60"

step() { echo; echo "=============== $* ==============="; }

sql() {
  local body
  body=$(python3 -c '
import json, sys
print(json.dumps({"warehouse_id": sys.argv[1], "statement": sys.argv[2],
                  "format": "JSON_ARRAY", "disposition": "INLINE",
                  "wait_timeout": "50s"}))' "$WAREHOUSE_ID" "$1")
  databricks api post /api/2.0/sql/statements -p "$PROFILE" --json "$body" \
    | python3 -c '
import json, sys
d = json.load(sys.stdin)
st = d.get("status", {}).get("state")
if st != "SUCCEEDED":
    print("SQL FAILED:", json.dumps(d.get("status", {}))[:600]); sys.exit(1)
for r in d.get("result", {}).get("data_array", []) or []:
    print("  ".join("NULL" if v is None else str(v) for v in r))
'
}

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

seed() {   # seed <lag> <spread> <events> <label>
  echo "  seeding: lag=${1}m spread=${2}m events=${3}  ($4)"
  databricks jobs run-now -p "$PROFILE" --timeout 20m --json "$(cat <<JSON
{
  "job_id": $SEED_JOB_ID,
  "only": ["seed"],
  "notebook_params": {
    "lag_minutes": "$1", "spread_minutes": "$2", "events_per_run": "$3",
    "catalog": "$CATALOG", "schema": "$SCHEMA"
  }
}
JSON
)" > /dev/null
}

# ------------------------------------------------------------------ 1. good code

# deploy.sh --fix rewrites the amount line in the working tree, so the baseline
# is correct regardless of the state the last rehearsal left behind. It is not
# read from git, which means a bug state accidentally committed at some point
# cannot poison the healthy baseline.
step "1/6  deploy good code"
./scripts/deploy.sh --fix --no-run

# The demo's on-camera fix is 'git checkout -- src/pipeline.py', which only works
# if the committed file is the good version. Warn, but do not block: the staging
# itself is unaffected, since step 1 just rewrote the line directly.
if ! git show HEAD:src/pipeline.py | grep -q 'cast("double") / 100'; then
  echo
  echo "WARNING: the COMMITTED src/pipeline.py is missing the '/ 100', so"
  echo "  'git checkout -- src/pipeline.py' would restore the bug, not the fix."
  echo "  Staging is fine. Before recording, commit the good version:"
  echo "    git commit -am 'restore amount conversion'"
  echo "  On camera, use './scripts/deploy.sh --fix' as the fix instead."
fi

# --------------------------------------------------------------- 2. healthy tail

step "2/6  clear landing, seed a contiguous healthy tail"
# DELETE, not TRUNCATE, and the pipeline tables are deliberately left alone.
#
# TRUNCATE gives landing a new Delta table id. Bronze's checkpoint still records
# the old id, so the next incremental update dies with
# DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE. Dropping the pipeline tables
# does not save you either: it clears their checkpoints, so the following update
# succeeds and the failure only surfaces one update later. DELETE keeps the table
# identity and just removes rows, and the --full-refresh in step 3 resets the
# checkpoints and operator state to match.
sql "DELETE FROM $CATALOG.$SCHEMA.landing_payments"
echo "  landing emptied"
for b in "${HEALTHY_BATCHES[@]}"; do
  # shellcheck disable=SC2086
  seed $b "healthy"
done

# --------------------------------------------------------------- 3. full refresh

step "3/6  full refresh to build the healthy baseline"
# --full-refresh is required, not just convenient. It is what resets the stream
# checkpoints and operator state to match the emptied landing table. Skipping it
# leaves bronze's checkpoint pointing at offsets that no longer exist.
databricks pipelines start-update "$PIPELINE_ID" -p "$PROFILE" \
  --full-refresh --cause API_CALL > /dev/null
wait_for_update "healthy update" || { echo "healthy update FAILED" >&2; exit 1; }

LAST_GOOD_UTC=$(date -u +'%Y-%m-%d %H:%M:%S')
echo "last healthy update: $LAST_GOOD_UTC UTC"

# ------------------------------------------------------------------ 4. ship bug

step "4/6  apply and deploy the bug"
./scripts/deploy.sh --bug --no-run

# --------------------------------------------------------------- 5. corrupt tail

step "5/6  seed the corrupt batch, then advance the watermark"
# shellcheck disable=SC2086
seed $CORRUPT_BATCH "corrupt"
# shellcheck disable=SC2086
seed $ADVANCE_BATCH "watermark advancer"

# ------------------------------------------------------------ 6. corrupt update

step "6/6  incremental update through the broken transform"
databricks pipelines start-update "$PIPELINE_ID" --cause API_CALL -p "$PROFILE" > /dev/null
wait_for_update "corrupt update" || {
  echo "The corrupt update FAILED. It is supposed to report COMPLETED." >&2
  echo "CANNOT_UPDATE_TABLE_SCHEMA here means the cast was dropped along with" >&2
  echo "the '/ 100', which makes the bug fail loudly instead of silently." >&2
  exit 1
}
FIRST_BAD_UTC=$(date -u +'%Y-%m-%d %H:%M:%S')

# ------------------------------------------------------------------- confirm it

step "confirming the spike is actually visible"
echo "Corrupt windows (avg_ticket > 1000). This MUST NOT be empty, or the"
echo "corruption is sitting in silver with nothing to see on the dashboard:"
sql "SELECT date_format(window_start, 'MM-dd HH:mm') AS window_start,
            sum(txn_count) AS txns,
            round(sum(total_amount), 2) AS settled,
            round(sum(total_amount) / sum(txn_count), 2) AS avg_ticket
     FROM $CATALOG.$SCHEMA.gold_merchant_5min
     GROUP BY window_start HAVING avg_ticket > 1000
     ORDER BY window_start"

echo
echo "Healthy windows for contrast (newest 5):"
sql "SELECT date_format(window_start, 'MM-dd HH:mm') AS window_start,
            sum(txn_count) AS txns,
            round(sum(total_amount), 2) AS settled,
            round(sum(total_amount) / sum(txn_count), 2) AS avg_ticket
     FROM $CATALOG.$SCHEMA.gold_merchant_5min
     GROUP BY window_start HAVING avg_ticket <= 1000
     ORDER BY window_start DESC LIMIT 5"

cat > "$BOUNDARY_FILE" <<EOF
# Rehearsal ground truth, written by reset.sh. The demo does not read this:
# it rediscovers the boundary from Delta history via scripts/rewind-points.sh.
last_good_update_utc=$LAST_GOOD_UTC
first_bad_update_utc=$FIRST_BAD_UTC
EOF

step "ready to record"
cat "$BOUNDARY_FILE"
echo
echo "Rewind target: any timestamp between those two updates. Confirm with"
echo "  ./scripts/rewind-points.sh"
echo
echo "The bug is an uncommitted edit, so on camera:"
echo "  git diff                        the one-line bug"
echo "  git checkout -- src/pipeline.py  the fix"
