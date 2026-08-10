# SDP Rewind & Replay demo

A payments pipeline that ships a one-line bug, inflates every settled amount by
10x, and recovers with the Rewind API without a full refresh.

Companion codebase for the Lakeflow SDP **Streaming Time Travel / Rewind API** Beta
technical blog. This README is a high-level overview of the project. The steps and
tooling for running the demo are provided separately.

## What gets deployed

- **Pipeline** `sdp-rewind-replay-payments`: bronze → silver → gold, time travel
  enabled, running continuous
- **Job** `sdp-rewind-setup` creates the catalog, schema, and landing table
- **Dashboard** Payments Settlement Monitor: settled dollars vs. transaction count

Seeding runs from a local script (`scripts/feed.py`) rather than a job.

## Scenario

```
landing_payments      amount_minor = 7969  (integer cents, i.e. $79.69)
      |
bronze_payments       ingest only, no logic
      |
silver_payments       amount_minor / 100   (the defect lives here: / 10 when broken)
      |
gold_merchant_5min    5-min tumbling window per merchant, 10-min watermark
```

The defect is a wrong divisor on one line in `silver_payments`. Both lines exist in
the file; exactly one is commented:

```python
# correct
.withColumn("amount", F.col("amount_minor").cast("double") / 100)
# defective deploy
.withColumn("amount", F.col("amount_minor").cast("double") / 10)
```

Every amount becomes 10x too large, yet the pipeline does not fail. It reports
COMPLETED and continues producing incorrect values. This is precisely the class of
problem the feature addresses: a deploy that is syntactically valid and
schema-compatible but semantically wrong.

The cast is what allows the defect to pass. With it, `amount` remains a `double`
and the schema is unchanged, so SDP has no basis to reject the deploy. Remove the
cast as well and the column becomes `bigint`, at which point SDP rejects the deploy
immediately with `CANNOT_UPDATE_TABLE_SCHEMA`. That contrast is the argument for the
feature: SDP already catches deploys that change a schema, and rewind addresses the
ones it cannot detect.

## What the dashboard shows

The dashboard plots settled dollars as bars against transaction count as a line.
In normal operation the two move together. Under the defect, settled dollars rise
sharply while the transaction line stays flat: the same payments, valued
incorrectly. On a linear axis the spike compresses every healthy window toward the
baseline, making the anomaly immediately visible.

`avg_ticket` is the diagnostic measure. An average ticket of $1,378 at a coffee
merchant is not a volume anomaly, it is a units defect.

Representative output from a run:

```
window_start          txns  settled       avg_ticket
2026-08-05T23:50:00    185     30400.83       164.33   (healthy)
2026-08-06T04:20:00    282   4501753.00     15963.66   (corrupt)
```

Transaction count is unremarkable: the same volume as a healthy window, valued
incorrectly. The figures above are from an earlier 100x run; a 10x defect shifts
the magnitude, not the shape.

## Recovery

Deploy the corrected code first. Rewind restores data, not code, so replaying
against the defective logic only reproduces the corruption. Once the fix is
deployed, rewind to a UTC boundary immediately before the corruption and replay.
On a continuous pipeline, replay is an ordinary update that resumes on its own.

Verified outcome: rows restored to the exact pre-incident baseline, zero duplicate
windows, and zero duplicate payment IDs, demonstrating exactly-once semantics
holding through a stateful aggregation across a rewind.

## Operational notes

Observed behaviors and constraints during the Beta:

- **Timestamp format.** `rewind_timestamp` requires `yyyy-MM-dd HH:mm:ss`, UTC,
  space-separated. ISO-8601 is rejected, and the resulting error misleadingly
  reports `BEYOND_RETENTION`, implying Delta log cleanup.
- **Timezone conversion.** `DESCRIBE HISTORY` returns commit times in the
  warehouse's local zone, while `rewind_timestamp` is interpreted as UTC. Using a
  value directly from history rewinds to the wrong moment, potentially hours off,
  with no error.
- **Cascade carries downstream tables.** Rewinding silver with `cascade: true`
  rewinds every table downstream of it, gold included, in the same operation.
  Name the root dataset where the defect lives and leave bronze out so the source
  is not re-read. Confirm both silver and gold landed with the `RESTORE` check
  below.
- **Rewind does not restore code.** Deploy the fix before replaying.
- **Rewind emits no events.** `DESCRIBE HISTORY` filtered for `RESTORE` is the only
  way to confirm what moved.
- **A failed rewind is not a no-op.** It can write `RESTORE` commits before failing.
- **Dry run is limited.** It returns no resolved rewind point and does not surface
  failures that the actual call encounters.
- **Auto Loader blocks whole-pipeline rewind** with `CF_TIME_TRAVEL_ERROR`. This
  demo uses a Delta landing table deliberately.
- **Frozen gold row counts are expected.** A watermarked aggregation emits only
  when a window closes, so counts remain steady while the pipeline is healthy. This
  is not a sign of a failed rewind.
- **Warehouse type.** Verification requires a serverless or pro warehouse; a
  2X-Small classic warehouse cannot parse `DESCRIBE HISTORY`.

## Layout

```
databricks.yml              bundle definition, dogfood target, demo variables
src/pipeline.py             bronze → silver → gold, the defect is one commented line
src/setup.py                catalog, schema, landing table
resources/pipeline.yml      pipeline with pipelines.timeTravel.enabled, continuous
resources/jobs.yml          setup job (seeding is a script, not a job)
resources/dashboard.yml     dashboard resource
resources/dashboard.lvdash.json
scripts/feed.py             the seeder: clears landing, then feeds events
scripts/rewind.py           rewind to a timestamp, verify, optionally replay
scripts/_config.py          reads demo config from the bundle
```

`resources/dashboard.lvdash.json` hardcodes the fully-qualified gold table: DAB
does not substitute `${var.*}` inside a `.lvdash.json`, so changing `catalog` or
`demo_schema` requires editing that file as well. This is noted in `databricks.yml`.
