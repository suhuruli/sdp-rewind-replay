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
#   2. truncate landing, then seed a contiguous healthy tail
#   3. FULL REFRESH with good code -> the whole tail materializes healthy, and
#      operator state is cleared so no corrupt partial windows survive
#   4. commit the bug, deploy
#   5. seed the corrupt batch, plus a later batch to advance the watermark
#   6. incremental update -> only the new rows go through the broken transform
#
# WHY LANDING IS TRUNCATED. Reusing whatever is already in landing is cheaper,
# but event time then has a multi-hour hole between the last rehearsal and this
# one, which shows up on the dashboard as disconnected islands. Truncating and
# seeding a fresh contiguous window makes every rehearsal identical and gives the
# clean "long flat stretch, then a cliff" shape the demo is built around.
#
# WHY THE CORRUPT TAIL NEEDS TWO BATCHES. Gold emits a window only once the
# watermark passes the window's end, and the watermark trails the largest event
# time seen by 10 minutes. A batch spanning S minutes therefore leaves all but
# its last S-10 minutes sitting in open windows: a single batch cannot close its
# own windows. The second batch, seeded at a much smaller lag, drags the
# watermark forward and closes the corrupt windows so the spike is visible. Skip
# it and the corruption is real, is sitting in silver, and shows up nowhere.
#
# GIT SAFETY. Step 4 commits, and to stop local history growing a bug commit per
# rehearsal this resets --hard to the demo-baseline tag first. That discards
# commits, so it refuses unless all of the following hold:
#   - the demo-baseline tag exists
#   - HEAD is demo-baseline, or exactly one commit ahead of it
#   - if ahead, that commit touches only src/pipeline.py
#   - nothing uncommitted outside src/pipeline.py
# The only thing it can then discard is a previous rehearsal's bug commit.
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
BASELINE_TAG="${BASELINE_TAG:-demo-baseline}"
BOUNDARY_FILE="$REPO/.demo-boundary"

# Healthy tail: three batches, each spreading 60 minutes of event time, at
# descending lag so they abut. Covers a contiguous 3 hours ending 90 minutes ago.
HEALTHY_BATCHES=("210 60 600" "150 60 600" "90 60 600")   # lag spread events

# Corrupt batch, then the watermark advancer. The advancer's own windows stay
# open, so it adds no second spike.
CORRUPT_BATCH="60 25 500"
ADVANCE_BATCH="10 5 120"

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

# ---------------------------------------------------------------- git preflight

if ! git rev-parse -q --verify "refs/tags/$BASELINE_TAG" >/dev/null; then
  echo "ERROR: tag '$BASELINE_TAG' does not exist." >&2
  echo "       Create it on the clean commit:  git tag $BASELINE_TAG" >&2
  exit 1
fi

AHEAD=$(git rev-list --count "$BASELINE_TAG"..HEAD)
if (( AHEAD > 1 )); then
  echo "ERROR: HEAD is $AHEAD commits ahead of '$BASELINE_TAG'." >&2
  echo "       Refusing to reset --hard; that would discard real work." >&2
  echo "       Move the tag forward if those commits are meant to stay." >&2
  exit 1
fi

if (( AHEAD == 1 )); then
  TOUCHED=$(git diff --name-only "$BASELINE_TAG"..HEAD)
  if [[ "$TOUCHED" != "src/pipeline.py" ]]; then
    echo "ERROR: the commit above '$BASELINE_TAG' touches more than the pipeline:" >&2
    echo "$TOUCHED" | sed 's/^/         /' >&2
    exit 1
  fi
fi

DIRTY=$(git status --porcelain | awk '{print $2}' | grep -v '^src/pipeline\.py$' || true)
if [[ -n "$DIRTY" ]]; then
  echo "ERROR: uncommitted changes outside src/pipeline.py:" >&2
  echo "$DIRTY" | sed 's/^/         /' >&2
  exit 1
fi

# The baseline must hold GOOD code. If a rehearsal's bug state ever gets
# committed into it, --bug becomes a no-op, the "corrupt" update is actually
# healthy, and the whole staging silently produces nothing to demo.
if ! git show "$BASELINE_TAG:src/pipeline.py" | grep -q 'cast("double") / 100'; then
  echo "ERROR: '$BASELINE_TAG' does not contain the correct amount conversion." >&2
  echo "       The bug state was committed into the baseline, so --bug would be" >&2
  echo "       a no-op. Restore the '/ 100' and move the tag before rehearsing." >&2
  exit 1
fi

echo "git preflight OK. Resetting to '$BASELINE_TAG'."
git reset --hard "$BASELINE_TAG" >/dev/null

# ------------------------------------------------------------------ 1. good code

step "1/6  deploy good code"
./scripts/deploy.sh --fix --no-run

# --------------------------------------------------------------- 2. healthy tail

step "2/6  truncate landing, seed a contiguous healthy tail"
sql "TRUNCATE TABLE $CATALOG.$SCHEMA.landing_payments"
echo "  landing truncated"
for b in "${HEALTHY_BATCHES[@]}"; do
  # shellcheck disable=SC2086
  seed $b "healthy"
done

# --------------------------------------------------------------- 3. full refresh

step "3/6  full refresh with good code"
echo "Rebuilds bronze/silver/gold from landing and clears operator state."
databricks pipelines start-update "$PIPELINE_ID" -p "$PROFILE" \
  --full-refresh --cause API_CALL > /dev/null
wait_for_update "full refresh" || { echo "full refresh FAILED" >&2; exit 1; }

LAST_GOOD_UTC=$(date -u +'%Y-%m-%d %H:%M:%S')
echo "last healthy update: $LAST_GOOD_UTC UTC"

# ------------------------------------------------------------------ 4. ship bug

step "4/6  commit and deploy the bug"
./scripts/deploy.sh --bug --no-run
git add src/pipeline.py
# Reads like ordinary housekeeping. That is why this class of bug ships at all.
git commit -q -m "silver: simplify amount normalization

Amount conversion was doing redundant arithmetic on a value the processor
already normalizes. Dropping it.

Co-authored-by: Isaac"
BUG_COMMIT=$(git rev-parse --short HEAD)
BUG_TIME_UTC=$(TZ=UTC git log -1 --date=iso-local --format=%ad)
echo "bug commit $BUG_COMMIT at $BUG_TIME_UTC"

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
bug_commit=$BUG_COMMIT
bug_commit_utc=$BUG_TIME_UTC
EOF

step "ready to record"
cat "$BOUNDARY_FILE"
echo
echo "Rewind target: any timestamp between those two updates. Confirm with"
echo "  ./scripts/rewind-points.sh"
echo
echo "The tree is on the bug commit, so 'git log' and 'git show' work on camera."
echo "The fix is 'git revert $BUG_COMMIT'."
