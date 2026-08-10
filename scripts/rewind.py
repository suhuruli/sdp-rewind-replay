#!/usr/bin/env python3
"""Rewind the pipeline to a UTC timestamp, then optionally replay.

    ./scripts/rewind.py --mark                               # print a UTC mark
    ./scripts/rewind.py --at '2026-08-08 01:25:59'           # rewind only
    ./scripts/rewind.py --at '2026-08-08 01:25:59' --replay  # rewind, verify, replay

You supply the timestamp; this never infers which update was the bad one. Inferring
it from position in Delta history ("newest STREAMING UPDATE") breaks on any extra
update: a replay, a second rehearsal, or the rewind's own RESTORE commit.

WHAT IT CHECKS, and why each one has already bitten this demo:

  1. Format. rewind_timestamp needs 'yyyy-MM-dd HH:mm:ss'. ISO-8601 is rejected
     and the error misreports it as BEYOND_RETENTION, blaming Delta log cleanup.
  2. The timestamp is UTC, while DESCRIBE HISTORY renders in the warehouse's
     local zone. Pasting a local time silently rewinds hours off target.
  3. The target lands strictly between commits, not inside one update's writes.
     Silver and gold do not share a version timeline: gold commits land seconds
     after silver's for the same batch, so a timestamp between them rewinds the
     two inconsistently.
  4. The DEPLOYED code, not the local file. Rewind restores data, never code.
     Replaying against a still-broken transform recreates the corruption, which
     is exactly what happened on 2026-08-07: a replay reproduced all four
     corrupt windows bit-identically because the fix was never deployed.
  5. Every dataset's RESTORE actually landed before replaying. This rewinds
     silver with `cascade: true`, so gold rewinds with it automatically, then
     confirms both moved via DESCRIBE HISTORY before replaying. Bronze is left
     out so the source is never re-read.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _config  # noqa: E402  (needs the path set above)

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def cli(args, parse_json=True):
    p = subprocess.run(
        ["databricks"] + args,
        capture_output=True, text=True,
        env={**os.environ, "DATABRICKS_AUTH_TYPE": "pat"},
    )
    if p.returncode != 0:
        raise RuntimeError("databricks %s failed: %s"
                           % (" ".join(args[:2]), (p.stderr or p.stdout)[:600]))
    return json.loads(p.stdout) if parse_json else p.stdout


def sql(statement, profile, warehouse_id):
    body = json.dumps({
        "warehouse_id": warehouse_id, "statement": statement,
        "format": "JSON_ARRAY", "disposition": "INLINE", "wait_timeout": "50s",
    })
    d = cli(["api", "post", "/api/2.0/sql/statements", "-p", profile,
             "--json", body, "-o", "json"])
    if d.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError("SQL failed: %s" % json.dumps(d.get("status", {}))[:500])
    return d.get("result", {}).get("data_array") or []


def history(table, catalog, schema, profile, warehouse_id, limit=25):
    """Commit history with timestamps converted to UTC.

    to_utc_timestamp(ts, current_timezone()) reinterprets the session-local
    commit time into UTC, the frame rewind_timestamp is read in.
    """
    rows = sql(
        "SELECT version, date_format(to_utc_timestamp(timestamp, "
        "current_timezone()), 'yyyy-MM-dd HH:mm:ss') AS utc, operation, "
        "operationMetrics['numOutputRows'] AS out FROM (DESCRIBE HISTORY "
        "%s.%s.%s) ORDER BY version DESC LIMIT %d"
        % (catalog, schema, table, limit),
        profile, warehouse_id)
    return [{"version": int(r[0]), "utc": r[1], "op": r[2] or "", "out": r[3]}
            for r in rows]


def validate_boundary(targets, ts, catalog, schema, profile, warehouse_id):
    """Confirm ts lands cleanly between commits on every table being rewound."""
    ok = True
    for t in targets:
        h = history(t, catalog, schema, profile, warehouse_id)
        before = [c for c in h if c["utc"] < ts]
        after = [c for c in h if c["utc"] > ts]
        print("  %s" % t)
        if not before:
            print("    NO commit before the target in the last %d versions." % len(h))
            print("    Target may predate available history, which fails as "
                  "BEYOND_RETENTION.")
            ok = False
        else:
            c = before[0]
            print("    last commit before : v%-3d %s  %s"
                  % (c["version"], c["utc"], c["op"]))
        if after:
            c = after[-1]
            print("    first commit after : v%-3d %s  %s"
                  % (c["version"], c["utc"], c["op"]))
            # A RESTORE after the target means this table was already rewound
            # past this point; rewinding again is untested territory.
            if any(c["op"] == "RESTORE" for c in after):
                print("    NOTE: a RESTORE already exists after this target.")
        else:
            print("    first commit after : none, target is at or beyond the "
                  "newest commit")
            print("    Nothing to undo on this table.")
            ok = False
    return ok


def deployed_divisor(profile):
    """Read the DEPLOYED notebook, not the local file. The gap between the two
    is what caused a replay to recreate the corruption.

    The path comes from the bundle rather than being written down here, so this
    works for whoever deployed it."""
    path = _config.deployed_notebook(profile, "src/pipeline")
    if not path:
        return None, "could not resolve the deployed notebook path"
    try:
        src = cli(["workspace", "export", path, "-p", profile], parse_json=False)
    except Exception as e:
        return None, str(e)[:200]
    for line in src.split("\n"):
        s = line.strip()
        if s.startswith('.withColumn("amount"'):
            m = re.search(r"/\s*(\d+)", s)
            return (m.group(1) if m else "?"), None
    return None, "no active amount line found"


def poll_update(pipeline_id, update_id, profile, label):
    """Poll by update_id, never latest_updates[0]: a feeder or a second operator
    can start an update that would otherwise be mistaken for this one."""
    while True:
        d = cli(["api", "get", "/api/2.0/pipelines/%s/updates/%s"
                 % (pipeline_id, update_id), "-p", profile, "-o", "json"])
        state = d.get("update", {}).get("state", "UNKNOWN")
        print("  %s: %s" % (label, state))
        if state == "COMPLETED":
            return True
        if state in ("FAILED", "CANCELED"):
            return False
        time.sleep(15)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--at", help="rewind timestamp, 'yyyy-MM-dd HH:mm:ss' UTC")
    ap.add_argument("--mark", action="store_true",
                    help="print the current UTC timestamp and exit")
    ap.add_argument("--profile", default=os.environ.get("DATABRICKS_PROFILE", "e2-dogfood"))
    # Which datasets to rewind stays a per-invocation choice: scoping is the whole
    # point of the feature. Everything else comes from databricks.yml.
    ap.add_argument("--dataset", default="silver_payments",
                    help="dataset to rewind (default silver_payments)")
    ap.add_argument("--downstream", default="gold_merchant_5min",
                    help="downstream dataset carried by cascade; verified after the rewind")
    ap.add_argument("--replay", action="store_true",
                    help="after a verified rewind, run the replay update")
    ap.add_argument("--yes", action="store_true", help="skip confirmation")
    args = ap.parse_args()

    # Before resolving config: --mark is just a clock read and should stay instant.
    if args.mark or not args.at:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        print(now)
        if not args.at:
            print("\nUse this as a boundary AFTER a healthy update completes, then:",
                  file=sys.stderr)
            print("  ./scripts/rewind.py --at '%s'" % now, file=sys.stderr)
        return 0

    try:
        cfg = _config.load(args.profile)
    except Exception as e:
        print("cannot read the bundle: %s" % e)
        return 1
    args.catalog = cfg["catalog"]
    args.schema = cfg["schema"]
    args.warehouse_id = cfg["warehouse_id"]
    args.pipeline_id = cfg["pipeline_id"]

    ts = args.at.strip()
    if not TS_RE.match(ts):
        print("BAD FORMAT: %r" % ts)
        print("Need 'yyyy-MM-dd HH:mm:ss' UTC, space-separated. ISO-8601 is")
        print("rejected, and the error misreports it as BEYOND_RETENTION.")
        return 1

    targets = [args.dataset, args.downstream]

    print("=== deployed code ===")
    div, err = deployed_divisor(args.profile)
    if err:
        print("  could not read deployed notebook: %s" % err)
    else:
        verdict = "GOOD" if div == "100" else "BUG"
        print("  active divisor in DEPLOYED silver: / %s   [%s]" % (div, verdict))
        if args.replay and div != "100":
            print()
            print("  REFUSING to replay: the deployed transform is still broken.")
            print("  Rewind restores data, never code. Replaying now recreates")
            print("  the corruption. Deploy the fix first, then re-run.")
            return 1

    print()
    print("=== boundary check for %s UTC ===" % ts)
    ok = validate_boundary(targets, ts, args.catalog, args.schema,
                           args.profile, args.warehouse_id)
    if not ok:
        print()
        print("Target is not a clean boundary. Nothing called.")
        if not args.yes:
            return 1
        print("Continuing anyway (--yes).")

    # Rewind the root dataset with `cascade: true` and every table downstream of
    # it rewinds with it: silver names the defect, gold follows automatically.
    # Bronze is deliberately left out so the source is never re-read. We still
    # verify both silver and gold landed (see the RESTORE check below), because
    # confirming what actually moved is worth doing however the rewind was issued.
    datasets = [{"identifier": "%s.%s.%s" % (args.catalog, args.schema, args.dataset),
                 "cascade": True}]

    payload = {"cause": "API_CALL",
               "rewind_spec": {"rewind_timestamp": ts, "datasets": datasets}}

    print()
    print("=== rewind call ===")
    print("  databricks pipelines start-update %s \\" % args.pipeline_id)
    print("    -p %s --json '%s'" % (args.profile, json.dumps(payload)))

    if not args.yes:
        try:
            if input("\nsend it? [y/N] ").strip().lower() not in ("y", "yes"):
                print("not sent")
                return 0
        except EOFError:
            print("\nnot sent (no tty; pass --yes to run unattended)")
            return 0

    print()
    d = cli(["pipelines", "start-update", args.pipeline_id, "-p", args.profile,
             "--json", json.dumps(payload), "-o", "json"])
    upd = d.get("update_id")
    print("rewind update %s started" % upd)
    if not poll_update(args.pipeline_id, upd, args.profile, "rewind"):
        print("\nRewind FAILED. Note a failed rewind is not a no-op: it can")
        print("write RESTORE commits before failing. Check history before retrying.")
        return 1

    print()
    print("=== did the rewind actually land? ===")
    # Rewind emits no rewind-specific events, so DESCRIBE HISTORY is the only
    # way to confirm what moved.
    restored = {}
    for t in targets:
        h = history(t, args.catalog, args.schema, args.profile, args.warehouse_id, 3)
        restored[t] = any(c["op"] == "RESTORE" for c in h)
        print("  %-20s %s" % (t, "RESTORE present" if restored[t] else "NO RESTORE"))
        for c in h:
            print("      v%-3d %s  %s" % (c["version"], c["utc"], c["op"]))

    if not all(restored.values()):
        print()
        print("STOP. Not every dataset was rewound.")
        print("Replaying into a half-rewound graph hard-blocks the pipeline with")
        print("a non-append-only source error and needs a full refresh. Check the")
        print("history above before doing anything else.")
        return 1

    if not args.replay:
        print()
        print("Rewind verified. Replay when ready (an ordinary update, no flags):")
        print("  databricks pipelines start-update %s -p %s"
              % (args.pipeline_id, args.profile))
        return 0

    print()
    print("=== replay ===")
    d = cli(["pipelines", "start-update", args.pipeline_id, "--cause", "API_CALL",
             "-p", args.profile, "-o", "json"])
    upd = d.get("update_id")
    print("replay update %s started" % upd)
    if not poll_update(args.pipeline_id, upd, args.profile, "replay"):
        print("Replay FAILED.")
        return 1

    print()
    print("=== correctness ===")
    rows = sql(
        "SELECT 'dupe_payment_ids' chk, count(*) v FROM (SELECT payment_id FROM "
        "{c}.{s}.silver_payments GROUP BY payment_id HAVING count(*)>1) "
        "UNION ALL SELECT 'dupe_gold_windows', count(*) FROM (SELECT merchant_id, "
        "window_start FROM {c}.{s}.gold_merchant_5min GROUP BY merchant_id, "
        "window_start HAVING count(*)>1) "
        "UNION ALL SELECT 'windows_still_inflated', count(*) FROM "
        "{c}.{s}.gold_merchant_5min WHERE avg_ticket>1000 "
        "UNION ALL SELECT 'approved_bronze_missing_from_silver', count(*) FROM ("
        "SELECT payment_id FROM {c}.{s}.bronze_payments WHERE "
        "coalesce(is_test,false)=false AND auth_result='approved' EXCEPT "
        "SELECT payment_id FROM {c}.{s}.silver_payments)".format(
            c=args.catalog, s=args.schema),
        args.profile, args.warehouse_id)
    bad = False
    for chk, v in rows:
        flag = "OK" if v == "0" else "FAIL"
        if v != "0":
            bad = True
        print("  %-38s %-6s %s" % (chk, v, flag))
    print()
    print("Replay complete." if not bad else "Replay completed with FAILED checks.")
    return 1 if bad else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\ninterrupted")
        sys.exit(130)
