#!/usr/bin/env python3
"""Continuously feed payment events into the landing table.

    ./scripts/feed.py                    # 30 min of feed, a batch every 30s
    ./scripts/feed.py --duration 0       # feed until ctrl-C
    ./scripts/feed.py --interval 0       # no throttle, as fast as inserts land
    ./scripts/feed.py --once             # single batch, then exit
    ./scripts/feed.py --prime 0          # no opening ramp, steady state from batch 1

The only seeder. It writes over the SQL statement API from the laptop, so a batch
lands in about a second, where the old notebook seed job spent 20-40s per batch
spinning up serverless compute.

IT EMPTIES THE LANDING TABLE FIRST, so every run produces one contiguous stretch
of event time. The cleanup is a DELETE rather than a TRUNCATE for a specific
reason documented on clear_landing(). Emptying the source leaves the pipeline's
checkpoints and the aggregation's operator state describing rows that no longer
exist, so a --full-refresh-all is required to match; this script prints the
command but deliberately does not run it.

Catalog, schema, landing table, warehouse and pipeline id all come from
databricks.yml via scripts/_config.py. There are no flags to override them: the
bundle is the single source of truth, so change it there.

EVENT TIME IS DECOUPLED FROM THE WALL CLOCK. Gold aggregates into 5-minute
tumbling windows behind a 10-minute watermark, and a window only emits once the
watermark passes its end. The watermark is derived from the data, not from
now(), so what closes a window is a later *event* time arriving, not real time
elapsing. The feeder therefore advances its own event-time cursor by `--spread`
minutes per batch and never waits for the clock to catch up.

That decoupling is what makes the demo watchable: the defaults run event time at
5x real time, so a gold window closes every 60 seconds instead of every 5
minutes, and the dashboard visibly fills while someone is looking at it.

IT OPENS HEAVY AND TAPERS. At steady state a batch spans less than one 5-minute
window, and the watermark trails by 10 minutes, so nothing is plottable until
about six batches in: three minutes of empty chart. The first batch therefore
spans --prime minutes of event time and halves each batch until it reaches
--spread, which closes a row of windows immediately and then glides into the
steady cadence. See ramp() for why the event counts taper along with the spans.

WHY THE DEFAULTS ARE 30s / 2.5 min. The two knobs trade off against each other:

    --interval  how often a batch lands, i.e. the ingest rate
    --spread    how much event time a batch covers, i.e. how fast windows close

Faster is not better. Unthrottled, this produces ~1,800 batches and 216k events
in half an hour and pushes event time nearly a week into the future, which
overwhelms the pipeline and makes the dashboard timestamps absurd. At 30s and
2.5 min it is ~60 batches, 7,200 events, a new closed window every minute, and
~216 approved txns per window, which matches the transaction volume in the
sample output in README.md. Changing `--spread` changes that per-window count,
so the README's numbers stop matching.

Other consequences worth knowing:

  * Event time still runs ahead of the wall clock, about 2 hours ahead by the end
    of a default 30-minute run. Expected and harmless, but window timestamps will
    read slightly in the future.
  * `--lag` only positions the very first batch. After that the cursor is
    self-propelled, so lag stops mattering.
  * Rewind is unaffected: `rewind_timestamp` is matched against Delta *commit*
    times, which are real wall-clock moments, not the event times in the rows.
    Correlating a gold window against the update that produced it still works;
    just do not expect the two clocks to agree.

The rolling cursor also means successive batches abut instead of overlapping,
which keeps the dashboard reading as one contiguous stream.
"""
import argparse
import json
import os
import random
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _config  # noqa: E402  (needs the path set above)

MERCHANTS = [
    ("M001", "Northwind Coffee", "food_beverage", 350, 2400),
    ("M002", "Aurora Cycles", "sporting_goods", 4500, 89000),
    ("M003", "Bellwether Books", "retail", 1200, 6500),
    ("M004", "Cedar & Pine Hardware", "home_improvement", 2200, 47000),
    ("M005", "Halcyon Pharmacy", "health", 800, 12000),
    ("M006", "Ridgeline Fuel", "fuel", 3500, 11000),
]
CURRENCIES = ["USD"] * 17 + ["CAD", "GBP", "EUR"]
NETWORKS = ["visa", "mastercard", "amex", "discover"]


def sql(statement, profile, warehouse_id):
    body = json.dumps({
        "warehouse_id": warehouse_id,
        "statement": statement,
        "format": "JSON_ARRAY",
        "disposition": "INLINE",
        "wait_timeout": "50s",
    })
    p = subprocess.run(
        ["databricks", "api", "post", "/api/2.0/sql/statements",
         "-p", profile, "--json", body, "-o", "json"],
        capture_output=True, text=True,
        env={**os.environ},
    )
    if p.returncode != 0:
        raise RuntimeError("CLI failed: %s" % (p.stderr or p.stdout)[:500])
    d = json.loads(p.stdout)
    state = d.get("status", {}).get("state")
    if state != "SUCCEEDED":
        raise RuntimeError("SQL %s: %s" % (state, json.dumps(d.get("status", {}))[:500]))
    return d.get("result", {}).get("data_array") or []


def sql_literal(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def make_batch(count, window_start, window_end):
    """Events spread uniformly across [window_start, window_end)."""
    span = (window_end - window_start).total_seconds()
    rows = []
    for i in range(count):
        mid, mname, cat, low, high = random.choice(MERCHANTS)
        ts = window_start + timedelta(seconds=span * i / max(count - 1, 1))
        rows.append([
            str(uuid.uuid4()), mid, mname, cat,
            random.randint(low, high),
            random.choice(CURRENCIES),
            random.choice(NETWORKS),
            random.choices(["approved", "declined", "referred"],
                           weights=[92, 7, 1])[0],
            random.random() < 0.02,
            # Naive UTC, no offset. With a '+00:00' suffix, to_timestamp() in
            # silver reinterprets the value into the session zone and every
            # event time shifts, so gold windows stop lining up with the
            # update timestamps a rewind is anchored against.
            ts.replace(tzinfo=None).isoformat(sep=" "),
        ])
    return rows


def insert(rows, table, profile, warehouse_id):
    values = ",".join(
        "(" + ",".join(sql_literal(v) for v in r) + ")" for r in rows
    )
    sql(
        "INSERT INTO %s (payment_id, merchant_id, "
        "merchant_name, merchant_category, amount_minor, currency, "
        "card_network, auth_result, is_test, event_time) VALUES %s"
        % (table, values),
        profile, warehouse_id,
    )


def ramp(prime_spread, steady_spread, steady_events):
    """Event-time spans for the opening batches, tapering to steady state.

    The problem this solves is a cold start. A steady-state batch spans less than
    one 5-minute window, and the watermark trails the largest event time by 10
    minutes, so the first window does not close until roughly six batches in:
    three minutes of an empty dashboard before anything is plottable.

    The first batch therefore covers a wide span of event time, which closes a
    row of windows immediately, and each subsequent batch halves until it reaches
    the steady spread. Halving rather than stepping straight down matters: it
    keeps the closed-window count climbing smoothly, so the chart reads as a
    stream filling in rather than a block appearing and then stalling.

    Event counts scale with the span to hold density constant. That is the point
    of tapering the two together: every window ends up with the same number of
    transactions whether it was primed or fed at steady state, so the healthy
    baseline is flat and the corruption spike is the only thing that stands out.
    Scaling the span alone would make the primed windows look busier.

    Yields (spread_minutes, events) and stops once at steady state; the caller
    repeats the last pair forever.
    """
    density = steady_events / steady_spread
    spread = float(prime_spread)
    while spread > steady_spread * 1.01:
        yield spread, max(int(round(spread * density)), 1)
        spread = max(spread / 2.0, steady_spread)
    yield steady_spread, steady_events


def clear_landing(table, profile, warehouse_id):
    """Empty the landing table so each run feeds one contiguous stretch.

    DELETE, never TRUNCATE. TRUNCATE assigns the table a new Delta table id while
    bronze's checkpoint still records the old one, so the next pipeline update
    dies with DIFFERENT_DELTA_TABLE_READ_BY_STREAMING_SOURCE. Dropping the
    pipeline tables does not rescue it either: that clears their checkpoints, so
    the next update succeeds and the failure only surfaces one update later.
    DELETE keeps the table identity and just removes rows.

    A full refresh is still required afterwards to reset the stream checkpoints
    and the aggregation's operator state to match the emptied source. That is the
    operator's job, not this script's, so it is only reported here.
    """
    before = sql("SELECT count(*) FROM %s" % table, profile, warehouse_id)
    n = int(before[0][0]) if before else 0
    sql("DELETE FROM %s" % table, profile, warehouse_id)
    print("cleared %s (%d rows removed)" % (table, n))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default=os.environ.get("DATABRICKS_PROFILE", "e2-dogfood"))
    ap.add_argument("--events", type=int, default=120,
                    help="events per batch (default 120)")
    ap.add_argument("--interval", type=float, default=30,
                    help="seconds between batches (default 30). 0 feeds as fast "
                         "as the warehouse accepts inserts, which is far more "
                         "data than a demo needs")
    ap.add_argument("--duration", type=float, default=30,
                    help="minutes to keep feeding before stopping (default 30; "
                         "0 means run until ctrl-C)")
    ap.add_argument("--lag", type=float, default=20,
                    help="minutes to place the FIRST batch behind now. After "
                         "that the event-time cursor is self-propelled, so this "
                         "only sets the starting point (default 20)")
    ap.add_argument("--spread", type=float, default=2.5,
                    help="minutes of event time each batch covers at steady state "
                         "(default 2.5). With --interval 30 this runs event time "
                         "at 5x real time, closing a 5-minute gold window every 60s")
    ap.add_argument("--prime", type=float, default=60,
                    help="minutes of event time the FIRST batch covers, halving "
                         "each batch until it reaches --spread (default 60, which "
                         "closes ~10 windows immediately). --prime 0 disables the "
                         "ramp and feeds at steady state from the start")
    ap.add_argument("--once", action="store_true", help="one batch, then exit")
    args = ap.parse_args()

    try:
        cfg = _config.load(args.profile)
    except Exception as e:
        print("cannot read the bundle: %s" % e)
        return 1
    table = cfg["landing_fqn"]
    warehouse_id = cfg["warehouse_id"]

    # The taper. Batch 1 covers --prime minutes of event time and halves from
    # there; the last pair is steady state and repeats for the rest of the run.
    prime = max(args.prime, args.spread)
    schedule = list(ramp(prime, args.spread, args.events))

    # Rolling cursor so consecutive batches abut instead of overlapping. Only the
    # starting point is tied to the wall clock; from here the cursor advances by
    # each batch's own span, regardless of how long the batch took to write.
    #
    # --lag positions the END of the first batch, so priming reaches further back
    # rather than overshooting into the future.
    cursor = datetime.now(timezone.utc) - timedelta(minutes=args.lag + prime)

    started = time.monotonic()
    deadline = started + args.duration * 60 if args.duration > 0 else None

    print("feeding %d events/batch, %g min event-time spread per batch, "
          "%s" % (args.events, args.spread,
                  "no sleep between batches" if args.interval <= 0
                  else "%gs between batches" % args.interval))
    print("stopping after %g min" % args.duration if deadline
          else "running until ctrl-C")
    if len(schedule) > 1:
        primed_windows = int((prime - 10) / 5)
        print("priming: first batch spans %g min of event time (%d events) and "
              "halves over %d batches" % (schedule[0][0], schedule[0][1],
                                          len(schedule) - 1))
        print("         ~%d gold windows should close on the first batch, so the "
              "chart is not empty" % max(primed_windows, 0))
    print("landing: %s   ctrl-C to stop" % table)
    print()

    # Always start empty. Feeding on top of a previous run leaves a hole in event
    # time between the two, and rows landing behind the already-advanced watermark
    # are dropped by the aggregation outright: the data is real in silver and shows
    # up nowhere in gold. Starting empty makes every run identical.
    try:
        clear_landing(table, args.profile, warehouse_id)
    except Exception as e:
        print("cleanup FAILED: %s" % str(e)[:300])
        print("refusing to feed on top of unknown state")
        return 1
    print("NOW FULL REFRESH THE PIPELINE, before or while this feeds:")
    print("  databricks pipelines start-update %s --full-refresh-all "
          "--cause API_CALL -p %s" % (cfg["pipeline_id"], args.profile))
    print("  (the emptied source and the old checkpoints disagree until you do)")
    print()

    n = 0
    events = 0
    step = 0
    while True:
        # Walk the taper, then hold on its last entry for the rest of the run. A
        # failed batch does not advance `step`, so the ramp is retried intact
        # rather than skipping a rung.
        batch_spread, batch_events = schedule[min(step, len(schedule) - 1)]
        start, end = cursor, cursor + timedelta(minutes=batch_spread)
        try:
            rows = make_batch(batch_events, start, end)
            insert(rows, table, args.profile, warehouse_id)
            n += 1
            events += len(rows)
            print("[%s] batch %d: %d events, event time %s..%s UTC%s"
                  % (datetime.now(timezone.utc).strftime("%H:%M:%S"), n,
                     len(rows), start.strftime("%Y-%m-%d %H:%M:%S"),
                     end.strftime("%H:%M:%S"),
                     "  (priming, %g min)" % batch_spread
                     if step < len(schedule) - 1 else ""))
            # Advance only on success, so a failed batch is retried over the same
            # event-time window instead of leaving a gap in the stream.
            cursor = end
            step += 1
        except Exception as e:
            # Never die on a transient warehouse or network error: this runs
            # unattended behind a demo.
            print("[%s] batch FAILED: %s"
                  % (datetime.now(timezone.utc).strftime("%H:%M:%S"), str(e)[:300]))

        if args.once:
            return

        if deadline and time.monotonic() >= deadline:
            print()
            print("reached the %g min limit: %d batches, %d events, event time "
                  "through %s UTC"
                  % (args.duration, n, events, cursor.strftime("%Y-%m-%d %H:%M:%S")))
            return

        # No wall-clock throttle. The watermark advances on event time, which the
        # cursor supplies directly, so there is nothing to wait for. Sleep only if
        # the operator explicitly asked to pace the feed.
        if args.interval > 0:
            # Do not overshoot the deadline while sleeping.
            nap = args.interval
            if deadline:
                nap = min(nap, max(deadline - time.monotonic(), 0))
            time.sleep(nap)


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except KeyboardInterrupt:
        print("\nstopped")
        sys.exit(0)
