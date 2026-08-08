# SDP Rewind & Replay demo

A payments pipeline that ships a one-line bug, inflates every settled amount by
10x, and recovers with the Rewind API without a full refresh.

Companion codebase for the Lakeflow SDP **Streaming Time Travel / Rewind API** Beta
technical blog.

## What gets deployed

- **Pipeline** `sdp-rewind-replay-payments`: bronze → silver → gold, time travel
  enabled, running **continuous**
- **Job** `sdp-rewind-setup` creates the catalog, schema, and landing table
- **Dashboard** Payments Settlement Monitor: settled dollars vs. transaction count

Seeding is not a job. It is `scripts/feed.py`, run from the laptop.

## Quick start

```bash
databricks bundle deploy -p e2-dogfood
databricks bundle run setup -p e2-dogfood
```

Deploying starts the pipeline: continuous mode runs itself, and no job triggers
updates any more. Then feed it. `feed.py` empties the landing table first, so a
full refresh is needed to bring the pipeline's checkpoints back in line with the
emptied source:

```bash
./scripts/feed.py &          # 30 min of data, a batch every 30s, then stops
databricks pipelines start-update <pipeline-id> --full-refresh-all \
  --cause API_CALL -p e2-dogfood
```

The feed prints that exact command, with the pipeline id filled in, right after it
clears the table. Leave the feed running during a rewind: new data still landing
while recovery happens is the point.

The first batch is deliberately heavy, spanning an hour of event time, then halves
each batch until it reaches the steady 2.5 minutes. A steady-state batch is
narrower than one gold window and the watermark trails by 10 minutes, so without
the ramp the dashboard sits empty for the first three minutes. Priming closes
about ten windows on the very first batch instead. `--prime 0` turns it off.

Configuration lives in `databricks.yml` (catalog, schema, landing table,
warehouse) and the scripts read it from there. There are no flags to override it:
the bundle is the single source of truth, so change it there.

Stop the pipeline when you are done, or it bills serverless indefinitely:

```bash
databricks pipelines stop <pipeline-id> -p e2-dogfood
```

## The demo

```
landing_payments      amount_minor = 7969  ← integer CENTS. That is $79.69.
      |
bronze_payments       ingest only, no logic
      |
silver_payments       amount_minor / 100   ← THE BUG LIVES HERE (/ 10 when broken)
      |
gold_merchant_5min    5-min tumbling window per merchant, 10-min watermark
```

The bug is a wrong divisor on one line in `silver_payments`. Both lines are in the
file; exactly one is commented:

```python
# good
.withColumn("amount", F.col("amount_minor").cast("double") / 100)
# bad deploy
.withColumn("amount", F.col("amount_minor").cast("double") / 10)
```

Every amount becomes 10x too large. **The pipeline does not fail.** It reports
COMPLETED and keeps producing wrong money, which is the entire reason this
feature exists.

A wrong divisor is a more honest bug than a deleted one: the line still reads like
a unit conversion, which is exactly why it survives review.

The cast is what makes it survive. Keep it and `amount` stays a `double`, the
schema is unchanged, and SDP has nothing to object to. Drop the cast too and the
column becomes `bigint`, so SDP rejects the deploy immediately with
`CANNOT_UPDATE_TABLE_SCHEMA`. That contrast is the argument for the feature: SDP
already catches bad deploys that change a schema, and rewind is for the ones it
cannot see.

To stage it: run `./scripts/feed.py` to build a healthy baseline, then swap which
`amount` line is commented in `src/pipeline.py`, redeploy, and let the feed keep
running. The corrupt windows appear within a minute or two. Because the pipeline
is continuous and the feed advances event time on its own, nothing has to be
triggered by hand.

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

Note `txns` is ordinary. Same transaction count as a healthy window, valued
wrong. The numbers above are from an earlier 100x run; a 10x bug shifts the
magnitude, not the shape.

### Recovery

Fix the code first. Rewind restores data, never code, so replaying against the
bug just recreates the corruption.

Swap which `amount` line is commented in `src/pipeline.py`, then:

```bash
databricks bundle deploy -p e2-dogfood
```

Then rewind. `./scripts/rewind.py --mark` prints a UTC boundary timestamp to use,
and `./scripts/rewind.py --at '<ts>'` validates the boundary, issues the rewind,
and verifies what moved:

```bash
./scripts/rewind.py --mark                     # a timestamp to rewind to
./scripts/rewind.py --at '2026-08-07 12:38:20' # rewind, then verify
```

Underneath it is this call, which is the thing worth reading:

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

Replay is an ordinary update, with no special flags. On a continuous pipeline it
resumes on its own; `--replay` runs it explicitly and re-checks correctness:

```bash
./scripts/rewind.py --at '<ts>' --replay   # all three checks must read 0
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
`rewind.py` converts for you.

**Name every affected dataset; do not trust `cascade: true`.** Automatic downstream
cascade is documented but does not currently happen. Rewinding only silver leaves
gold un-rewound; the next update fails with `DELTA_SOURCE_IGNORE_DELETE` and the
pipeline is hard-blocked until a full refresh, the exact outcome the feature exists
to avoid. `rewind.py` therefore always names silver and gold explicitly, and leaves
bronze out so the source is never re-read.

**Rewind does not restore code.** Deploy your fix yourself before replaying.

**Rewind emits no events.** `DESCRIBE HISTORY` looking for `RESTORE` is the only way
to confirm what moved. That is what `rewind.py` does after every rewind.

**A failed rewind is not a no-op.** It can write `RESTORE` commits before failing.

**Dry run is weak.** No resolved rewind point returned, and it misses failures the
real call hits.

**Auto Loader blocks whole-pipeline rewind** with `CF_TIME_TRAVEL_ERROR`. This demo
uses a Delta landing table deliberately.

**Gold row counts freezing is normal.** A watermarked aggregation only emits when a
window closes, so counts stall while the pipeline is healthy. A smaller `--spread`
in feed.py closes them sooner. Do not read frozen counts as a broken rewind.

**Warehouse choice.** `rewind.py` needs serverless or pro; a 2X-Small classic
warehouse cannot parse `DESCRIBE HISTORY`.

## Layout

```
databricks.yml              bundle definition, dogfood target, demo variables
src/pipeline.py             bronze → silver → gold, the bug is one commented line
src/setup.py                catalog, schema, landing table
resources/pipeline.yml      pipeline with pipelines.timeTravel.enabled, continuous
resources/jobs.yml          setup job (seeding is a script, not a job)
resources/dashboard.yml     dashboard resource
resources/dashboard.lvdash.json
scripts/feed.py             the seeder: clears landing, then feeds events
scripts/rewind.py           rewind to a timestamp, verify, optionally replay
scripts/_config.py          reads demo config out of the bundle
research/FINDINGS.md        verified Beta behaviors and gotchas
```

`resources/dashboard.lvdash.json` hardcodes the fully-qualified gold table:
DAB does not substitute `${var.*}` inside a `.lvdash.json`, so changing `catalog`
or `demo_schema` means editing that file too. Noted in `databricks.yml`.

## Teardown

```bash
databricks bundle destroy -p e2-dogfood
# Pipeline tables are not bundle-managed:
#   DROP TABLE harsha_rewind_demo.payments.{bronze_payments,silver_payments,gold_merchant_5min}
#   DROP TABLE harsha_rewind_demo.payments.landing_payments
```
