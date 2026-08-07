#!/bin/bash
# Rebuild the demo's opening state: a long healthy history, then a corrupt tail.
#
#   ./reset.sh
#
# Leaves the repo and the workspace ready to record, with the bug already shipped
# and already visible on the dashboard. Nothing during the recording stages an
# event; every artifact the demo points at is real and already there.
#
# Five steps:
#   1. restore good code and deploy
#   2. FULL REFRESH -> rebuilds bronze/silver/gold from all of landing as healthy,
#      and clears operator state so no corrupt partial windows survive
#   3. seed one batch, advancing event time past the healthy tail
#   4. commit the bug and deploy
#   5. incremental update -> only the new rows go through the broken transform
#
# Landing is the source, not a pipeline table, so the full refresh recreates the
# whole healthy history for free. There is no need to reseed it. Nice side
# effect: yesterday's corrupt-era rows are raw cents in landing and always were,
# so each rebuild folds them back in as healthy. The flat stretch grows every
# time you rehearse and the spike stays pinned to the newest windows.
#
# GIT SAFETY. Step 4 commits, and to keep local history from growing a new bug
# commit per rehearsal this script resets --hard to the demo-baseline tag first.
# That discards commits, so it refuses to run unless all of the following hold:
#   - the demo-baseline tag exists
#   - HEAD is demo-baseline, or exactly one commit ahead of it
#   - if ahead, that one commit touches only src/pipeline.py
#   - no unstaged changes to anything other than src/pipeline.py
# Under those conditions the only thing it can discard is a previous rehearsal's
# bug commit. Anything else and it stops and explains why.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

PROFILE="${DATABRICKS_PROFILE:-e2-dogfood}"
export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"
PIPELINE_ID="${PIPELINE_ID:-87ba1cf1-9a9c-4750-8ad0-92690f61ae2c}"
BASELINE_TAG="${BASELINE_TAG:-demo-baseline}"

# Event-time lag for the corrupt batch. Must be greater than the 5-minute window
# plus the 10-minute watermark, or the corrupt windows stay open and the spike is
# invisible: gold only emits a window once the watermark passes its end.
BUG_LAG_MINUTES="${BUG_LAG_MINUTES:-25}"

SEED_JOB_ID="${SEED_JOB_ID:-972844999617450}"
BOUNDARY_FILE="$REPO/.demo-boundary"

step() { echo; echo "=============== $* ==============="; }

# ---------------------------------------------------------------- git preflight

if ! git rev-parse -q --verify "refs/tags/$BASELINE_TAG" >/dev/null; then
  echo "ERROR: tag '$BASELINE_TAG' does not exist." >&2
  echo "       Create it on the clean commit first:  git tag $BASELINE_TAG" >&2
  exit 1
fi

AHEAD=$(git rev-list --count "$BASELINE_TAG"..HEAD)
if (( AHEAD > 1 )); then
  echo "ERROR: HEAD is $AHEAD commits ahead of '$BASELINE_TAG'." >&2
  echo "       Refusing to reset --hard: that would discard real work." >&2
  echo "       Move the tag forward if these commits are meant to be permanent." >&2
  exit 1
fi

if (( AHEAD == 1 )); then
  TOUCHED=$(git diff --name-only "$BASELINE_TAG"..HEAD)
  if [[ "$TOUCHED" != "src/pipeline.py" ]]; then
    echo "ERROR: the commit above '$BASELINE_TAG' touches more than the pipeline:" >&2
    echo "$TOUCHED" | sed 's/^/         /' >&2
    echo "       Refusing to discard it." >&2
    exit 1
  fi
fi

DIRTY=$(git status --porcelain | awk '{print $2}' | grep -v '^src/pipeline\.py$' || true)
if [[ -n "$DIRTY" ]]; then
  echo "ERROR: uncommitted changes outside src/pipeline.py:" >&2
  echo "$DIRTY" | sed 's/^/         /' >&2
  echo "       Commit or stash them first." >&2
  exit 1
fi

echo "git preflight OK. Resetting to '$BASELINE_TAG' (discards prior bug commit)."
git reset --hard "$BASELINE_TAG" >/dev/null

# ------------------------------------------------------------------ 1. good code

step "1/5  deploying good code"
./scripts/deploy.sh --fix --no-run

# --------------------------------------------------------------- 2. full refresh

step "2/5  full refresh (rebuild all history as healthy)"
echo "Recomputes bronze/silver/gold from everything in landing and clears"
echo "operator state. This is what erases the previous rehearsal's corruption."

databricks pipelines start-update "$PIPELINE_ID" -p "$PROFILE" \
  --full-refresh --cause API_CALL > /dev/null

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
wait_for_update "full refresh" || { echo "full refresh FAILED" >&2; exit 1; }

# The boundary the demo has to rediscover from Delta history. Recorded here only
# as rehearsal ground truth, so you can check the on-camera answer is right.
LAST_GOOD_UTC=$(date -u +'%Y-%m-%d %H:%M:%S')
echo "last healthy update completed: $LAST_GOOD_UTC UTC"

# ------------------------------------------------------------------- 3. new data

step "3/5  seeding the batch that will be corrupted"
echo "lag ${BUG_LAG_MINUTES}m, so these windows close and the spike is visible."

RUN_ID=$(databricks jobs run-now -p "$PROFILE" --timeout 20m -o json --json "$(cat <<JSON
{
  "job_id": $SEED_JOB_ID,
  "only": ["seed"],
  "notebook_params": {
    "lag_minutes": "$BUG_LAG_MINUTES",
    "spread_minutes": "12",
    "events_per_run": "400"
  }
}
JSON
)" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print(''); raise SystemExit
print(d.get('run_id') or (d.get('metadata') or {}).get('run_id') or '')
")
echo "seed run: ${RUN_ID:-unknown}"

# ------------------------------------------------------------------ 4. ship bug

step "4/5  committing and deploying the bug"

./scripts/deploy.sh --bug --no-run

git add src/pipeline.py
# Reads like ordinary housekeeping, which is why this class of bug ships at all.
git commit -q -m "silver: simplify amount normalization

Amount conversion was doing redundant arithmetic on a value the processor
already normalizes. Dropping it.

Co-authored-by: Isaac"
BUG_COMMIT=$(git rev-parse --short HEAD)
BUG_COMMIT_TIME=$(git log -1 --date=iso-local --format=%ad)
echo "bug commit: $BUG_COMMIT at $BUG_COMMIT_TIME (local)"

# ------------------------------------------------------------- 5. corrupt update

step "5/5  incremental update through the broken transform"
databricks pipelines start-update "$PIPELINE_ID" --cause API_CALL -p "$PROFILE" > /dev/null
wait_for_update "corrupt update" || {
  echo "The corrupt update FAILED. It is supposed to report COMPLETED." >&2
  echo "If this says CANNOT_UPDATE_TABLE_SCHEMA the cast was dropped along" >&2
  echo "with the '/ 100', which makes the bug fail loudly instead of silently." >&2
  exit 1
}
FIRST_BAD_UTC=$(date -u +'%Y-%m-%d %H:%M:%S')

cat > "$BOUNDARY_FILE" <<EOF
# Rehearsal ground truth. Written by reset.sh, not used by the demo.
# The demo rediscovers this from Delta history via scripts/rewind-points.sh.
last_good_update_utc=$LAST_GOOD_UTC
first_bad_update_utc=$FIRST_BAD_UTC
bug_commit=$BUG_COMMIT
bug_commit_time_local=$BUG_COMMIT_TIME
EOF

step "ready to record"
cat "$BOUNDARY_FILE"
echo
echo "Rewind to a timestamp between those two. Confirm with:"
echo "  ./scripts/rewind-points.sh"
echo
echo "Working tree is on the bug commit, so 'git log' and 'git show' both work"
echo "on camera. The fix is 'git revert $BUG_COMMIT'."
