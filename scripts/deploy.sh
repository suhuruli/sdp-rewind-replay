#!/bin/bash
# Ship a code version of the pipeline, then run it.
#
#   ./deploy.sh --bug     ship the bad deploy (MINOR_UNITS_PER_DOLLAR = 1.0)
#   ./deploy.sh --fix     ship the fix        (MINOR_UNITS_PER_DOLLAR = 100.0)
#   ./deploy.sh           deploy current source as-is and run
#
# This edits src/pipeline.py in place so the bad deploy is a real one-line code
# change, visible in `git diff`, rather than a config toggle. Rewind restores
# data, never code, so --fix must be run before replaying.
#
# The edited line, inside silver_payments():
#
#   good:  .withColumn("amount", F.col("amount_minor").cast("double") / 100)
#   bug:   .withColumn("amount", F.col("amount_minor").cast("double"))
#
# The cast stays in the bad version on purpose. It keeps `amount` a double, so
# the schema is unchanged and SDP has no reason to reject the deploy.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$REPO/src/pipeline.py"
PROFILE="${DATABRICKS_PROFILE:-e2-dogfood}"
export DATABRICKS_AUTH_TYPE="${DATABRICKS_AUTH_TYPE:-pat}"

GOOD='        .withColumn("amount", F.col("amount_minor").cast("double") / 100)'
BUG='        .withColumn("amount", F.col("amount_minor").cast("double"))'

# Rewrite the amount line to $1. Fails loudly if the line is not found exactly
# once, so a rename upstream can never silently turn a bad deploy into a no-op.
set_amount_line() {
  python3 - "$SRC" "$1" <<'PY'
import sys
path, want = sys.argv[1], sys.argv[2]
lines = open(path).read().split("\n")
hits = [i for i, l in enumerate(lines)
        if l.lstrip().startswith('.withColumn("amount"')]
if len(hits) != 1:
    sys.exit("expected exactly 1 amount line in %s, found %d" % (path, len(hits)))
lines[hits[0]] = want
open(path, "w").write("\n".join(lines))
PY
}

RUN=true
while [[ "${1:-}" == --* ]]; do
  case "$1" in
    --bug)
      set_amount_line "$BUG"
      echo ">>> BAD DEPLOY staged. The '/ 100' is gone:"
      echo "    Amounts stay in cents but are labeled dollars. Every value 100x."
      echo "    The pipeline will NOT fail. That is the whole point."
      shift ;;
    --fix)
      set_amount_line "$GOOD"
      echo ">>> FIX staged. The '/ 100' is back."
      shift ;;
    --no-run) RUN=false; shift ;;
    *) echo "usage: $0 [--bug|--fix] [--no-run]" >&2; exit 1 ;;
  esac
done

echo
grep -n 'withColumn("amount"' "$SRC"
echo

databricks bundle deploy -p "$PROFILE"

if [[ "$RUN" == false ]]; then
  echo "Deployed, not run."
  exit 0
fi

PIPELINE_ID="${PIPELINE_ID:-87ba1cf1-9a9c-4750-8ad0-92690f61ae2c}"
echo
echo "--- running pipeline ---"
databricks pipelines start-update "$PIPELINE_ID" --cause API_CALL -p "$PROFILE" > /dev/null

while true; do
  state=$(databricks pipelines get "$PIPELINE_ID" -p "$PROFILE" 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
print((d.get('latest_updates') or [{}])[0].get('state', 'UNKNOWN'))
")
  printf '\r  update: %-24s' "$state"
  case "$state" in
    COMPLETED) echo; echo "Update COMPLETED."; break ;;
    FAILED|CANCELED) echo; echo "Update $state." >&2; exit 1 ;;
  esac
  sleep 15
done
