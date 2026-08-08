# SDP Rewind & Replay demo

A payments pipeline that ships a one-line bug, inflates every settled amount by
10x, and recovers with the Rewind API without a full refresh.

Companion codebase for the Lakeflow SDP **Streaming Time Travel / Rewind API** Beta
technical blog. This README is a high-level overview of the project. The steps and
tooling for running the demo are shared separately.

## What gets deployed

- **Pipeline** `sdp-rewind-replay-payments`: bronze → silver → gold, time travel
  enabled, running **continuous**
- **Job** `sdp-rewind-setup` creates the catalog, schema, and landing table
- **Dashboard** Payments Settlement Monitor: settled dollars vs. transaction count

Seeding is not a job. It is `scripts/feed.py`, run from the laptop.

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
bug just recreates the corruption. Once the fix is redeployed, rewind to a UTC
boundary just before the corruption and replay. On a continuous pipeline replay
is an ordinary update that resumes on its own.

The verified outcome: rows restored to the exact pre-incident baseline, **0**
duplicate windows, **0** duplicate payment IDs. Exactly-once holding through a
stateful aggregation across a rewind.

## Things that will bite you

**Timestamp format.** `rewind_timestamp` needs `yyyy-MM-dd HH:mm:ss`, UTC,
space-separated. ISO-8601 is rejected and the error misleadingly reports
`BEYOND_RETENTION`, blaming Delta log cleanup.

**Timestamps need converting.** `DESCRIBE HISTORY` returns commit times in the
warehouse's local zone, while `rewind_timestamp` is read as UTC. Pasting straight
out of history rewinds to the wrong moment, potentially hours off, with no error.

**Name every affected dataset; do not trust `cascade: true`.** Automatic downstream
cascade is documented but does not currently happen. Rewinding only silver leaves
gold un-rewound; the next update fails with `DELTA_SOURCE_IGNORE_DELETE` and the
pipeline is hard-blocked until a full refresh, the exact outcome the feature exists
to avoid. Name silver and gold explicitly, and leave bronze out so the source is
never re-read.

**Rewind does not restore code.** Deploy your fix yourself before replaying.

**Rewind emits no events.** `DESCRIBE HISTORY` looking for `RESTORE` is the only way
to confirm what moved.

**A failed rewind is not a no-op.** It can write `RESTORE` commits before failing.

**Dry run is weak.** No resolved rewind point returned, and it misses failures the
real call hits.

**Auto Loader blocks whole-pipeline rewind** with `CF_TIME_TRAVEL_ERROR`. This demo
uses a Delta landing table deliberately.

**Gold row counts freezing is normal.** A watermarked aggregation only emits when a
window closes, so counts stall while the pipeline is healthy. Do not read frozen
counts as a broken rewind.

**Warehouse choice.** Verification needs serverless or pro; a 2X-Small classic
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
```

`resources/dashboard.lvdash.json` hardcodes the fully-qualified gold table:
DAB does not substitute `${var.*}` inside a `.lvdash.json`, so changing `catalog`
or `demo_schema` means editing that file too. Noted in `databricks.yml`.
