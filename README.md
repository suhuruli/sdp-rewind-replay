# SDP Rewind & Replay demo

A payments pipeline that ships a one-line bug, inflates every settled amount by
100x, and recovers with the Rewind API without a full refresh.

Companion codebase for the Lakeflow SDP **Streaming Time Travel / Rewind API** Beta
technical blog.

## What gets deployed

- **Pipeline** `sdp-rewind-replay-payments`: bronze → silver → gold, time travel enabled
- **Job** `sdp-rewind-setup` creates the catalog, schema, and landing table
- **Job** `sdp-rewind-seed-stream` seeds data every 5 min then runs the pipeline (paused on deploy)
- **Dashboard** Payments Settlement Monitor: settled dollars vs. transaction count

## Quick start

```bash
databricks bundle deploy -p e2-dogfood
databricks bundle run setup -p e2-dogfood

# Seed a few batches at staggered lags so several windows close immediately.
# Use `jobs run-now` with notebook_params, not `bundle run --params`: the tasks
# use task-level base_parameters, so job-level params are rejected.
SEED_JOB=$(databricks bundle summary -p e2-dogfood -o json \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['resources']['jobs']['seed_stream']['id'])")
for lag in 60 45 30; do
  databricks jobs run-now -p e2-dogfood --timeout 15m --json "{
    \"job_id\": $SEED_JOB, \"only\": [\"seed\"],
    \"notebook_params\": {\"lag_minutes\": \"$lag\"}}"
done

# First update. Also satisfies the second rewind prerequisite.
databricks pipelines start-update <pipeline-id> --cause API_CALL -p e2-dogfood

./scripts/verify.sh
```

Then unpause `sdp-rewind-seed-stream` so data keeps arriving and rewind points
accumulate. Leave it running during a rewind: new data still landing while recovery
happens is the point.

## The demo

```
landing_payments      amount_minor = 7969  ← integer CENTS. That is $79.69.
      |
bronze_payments       ingest only, no logic
      |
silver_payments       amount_minor / 100   ← THE BUG LIVES HERE
      |
gold_merchant_5min    5-min tumbling window per merchant, 10-min watermark
```

The bug is the deletion of `/ 100` from one line in `silver_payments`:

```python
# good
.withColumn("amount", F.col("amount_minor").cast("double") / 100)
# bad deploy
.withColumn("amount", F.col("amount_minor").cast("double"))
```

Every amount becomes 100x too large. **The pipeline does not fail.** It reports
COMPLETED and keeps producing wrong money, which is the entire reason this
feature exists.

The cast is what makes it survive. Keep it and `amount` stays a `double`, the
schema is unchanged, and SDP has nothing to object to. Drop the cast too and the
column becomes `bigint`, so SDP rejects the deploy immediately with
`CANNOT_UPDATE_TABLE_SCHEMA`. That contrast is the argument for the feature: SDP
already catches bad deploys that change a schema, and rewind is for the ones it
cannot see.

`./scripts/reset.sh` stages the whole scenario, so the corruption is already
visible before you start. Roughly 3 minutes.

### The aha moment

The dashboard plots settled dollars as bars against transaction count as a line. In
normal operation they move together. Under the bug, dollars explode while the
transaction line stays flat: **the same payments, valued wrong.** On a linear axis
the spike flattens every healthy window into a baseline, so the chart itself visibly
breaks.

`avg_ticket` is the diagnostic column. A coffee shop at a $1,378 average ticket is
not a volume anomaly, it is a units bug.

Measured, from a real run:

```
window_start          txns  settled       avg_ticket
2026-08-05T23:50:00    185     30400.83       164.33   ← healthy
2026-08-06T04:20:00    282   4501753.00     15963.66   ← corrupt
```

Note `txns` is ordinary. Same transaction count as a healthy window, valued 100x
wrong.

### Recovery

Fix the code first. Rewind restores data, never code, so replaying against the
bug just recreates the corruption.

```bash
git checkout -- src/pipeline.py    # the fix
databricks bundle deploy -p e2-dogfood
```

Then get the rewind command, with a real timestamp already filled in:

```bash
./scripts/rewind-points.sh
```

It prints the call for you to run by hand rather than wrapping it, because the
API call is the thing worth reading:

```bash
databricks pipelines start-update <pipeline-id> -p e2-dogfood --json '{
  "cause": "API_CALL",
  "rewind_spec": {
    "rewind_timestamp": "2026-08-07 12:38:20",
    "datasets": [
      { "identifier": "harsha_rewind_demo.payments.silver_payments" },
      { "identifier": "harsha_rewind_demo.payments.gold_merchant_5min" }
    ]
  }
}'
```

Replay is an ordinary update, with no special flags:

```bash
databricks pipelines start-update <pipeline-id> -p e2-dogfood
./scripts/verify.sh    # all three correctness checks must read 0
```

Verified outcome: rows restored to the exact pre-incident baseline, **0** duplicate
windows, **0** duplicate payment IDs. Exactly-once holding through a stateful
aggregation across a rewind.

## Things that will bite you

Full detail in [research/FINDINGS.md](research/FINDINGS.md).

**Timestamp format.** `rewind_timestamp` needs `yyyy-MM-dd HH:mm:ss`, UTC,
space-separated. ISO-8601 is rejected and the error misleadingly reports
`BEYOND_RETENTION`, blaming Delta log cleanup.

**Timestamps need converting.** `DESCRIBE HISTORY` returns commit times in the
warehouse's local zone, while `rewind_timestamp` is read as UTC. Pasting straight
out of history rewinds to the wrong moment, potentially hours off, with no error.
`rewind-points.sh` converts for you.

**Name every affected dataset.** Automatic downstream cascade is documented but does
not currently happen. Rewinding only silver leaves gold un-rewound; the next update
fails with `DELTA_SOURCE_IGNORE_DELETE` and the pipeline is hard-blocked until a full
refresh, the exact outcome the feature exists to avoid. Name silver and gold, and
leave bronze out so the source is never re-read.

**Rewind does not restore code.** Deploy your fix yourself before replaying.

**Rewind emits no events.** `DESCRIBE HISTORY` looking for `RESTORE` is the only way
to confirm what moved. That is what `verify.sh` does.

**A failed rewind is not a no-op.** It can write `RESTORE` commits before failing.

**Dry run is weak.** No resolved rewind point returned, and it misses failures the
real call hits.

**Auto Loader blocks whole-pipeline rewind** with `CF_TIME_TRAVEL_ERROR`. This demo
uses a Delta landing table deliberately.

**Gold row counts freezing is normal.** A watermarked aggregation only emits when a
window closes, so counts stall while the pipeline is healthy. Lower `lag_minutes` to
close them. Do not read frozen counts as a broken rewind.

**Warehouse choice.** `verify.sh` needs serverless or pro; a 2X-Small classic
warehouse cannot parse `DESCRIBE HISTORY`.

## Layout

```
databricks.yml              bundle definition, dogfood target
src/pipeline.py             bronze → silver → gold, bug behind a config flag
src/seed.py                 payment event generator
src/setup.py                catalog, schema, landing table
resources/pipeline.yml      pipeline with pipelines.timeTravel.enabled
resources/jobs.yml          setup + seed_stream jobs
resources/dashboard.yml     dashboard resource
resources/dashboard.lvdash.json
scripts/reset.sh            stage the scenario: healthy history, then corruption
scripts/deploy.sh           --bug / --fix the one line, deploy, run
scripts/rewind-points.sh    print the rewind command with a UTC timestamp
scripts/verify.sh           counts, Delta history, windows, correctness checks
research/FINDINGS.md        verified Beta behaviors and gotchas
```

## Teardown

```bash
databricks bundle destroy -p e2-dogfood
# Pipeline tables are not bundle-managed:
#   DROP TABLE harsha_rewind_demo.payments.{bronze_payments,silver_payments,gold_merchant_5min}
#   DROP TABLE harsha_rewind_demo.payments.landing_payments
```
