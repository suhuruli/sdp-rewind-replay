#!/usr/bin/env python3
"""Read demo configuration out of the bundle instead of hardcoding it.

feed.py and rewind.py both need the same catalog, schema, landing table,
warehouse, and pipeline id. Those already live in databricks.yml or are produced
by deploying it, so duplicating them as script defaults just gave them somewhere
to rot: the pipeline id in particular changes whenever the pipeline is recreated,
and a stale one sends a rewind at the wrong pipeline.

databricks.yml is the single source of truth and the scripts take no flags to
override it. To point the demo somewhere else, edit the variable.

There are two lookups, because DAB treats inputs and outputs differently:

    variables  declared in databricks.yml   -> `bundle validate -o json`
    ids/paths  assigned by deploying        -> `bundle summary -o json`
"""
import functools
import json
import os
import subprocess

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _bundle(subcommand, profile):
    p = subprocess.run(
        ["databricks", "bundle", subcommand, "-p", profile, "-o", "json"],
        capture_output=True, text=True, cwd=REPO,
        env={**os.environ, "DATABRICKS_AUTH_TYPE": "pat"},
    )
    if p.returncode != 0:
        raise RuntimeError((p.stderr or p.stdout).strip()[:300])
    return json.loads(p.stdout)


# Two CLI calls at under a second each, so resolve once per process.
@functools.lru_cache(maxsize=None)
def load(profile):
    """Resolved demo config, or a clear error. Nothing is guessed: a wrong
    pipeline id is worse than a script that refuses to start."""
    v = _bundle("validate", profile).get("variables") or {}
    cfg = {}
    for key, var in (("catalog", "catalog"), ("schema", "demo_schema"),
                     ("landing_table", "landing_table"),
                     ("warehouse_id", "warehouse_id")):
        value = (v.get(var) or {}).get("value")
        if not value:
            raise RuntimeError("databricks.yml is missing variable %r" % var)
        cfg[key] = value

    pipeline = ((_bundle("summary", profile).get("resources") or {})
                .get("pipelines") or {}).get("payments_pipeline") or {}
    if not pipeline.get("id"):
        raise RuntimeError("no deployed pipeline found; run "
                           "`databricks bundle deploy` first")
    cfg["pipeline_id"] = pipeline["id"]
    cfg["landing_fqn"] = "%s.%s.%s" % (cfg["catalog"], cfg["schema"],
                                       cfg["landing_table"])
    return cfg


def deployed_notebook(profile, relative_path):
    """Workspace path of a deployed source file, e.g. 'src/pipeline'."""
    try:
        libs = (((_bundle("summary", profile).get("resources") or {})
                 .get("pipelines") or {}).get("payments_pipeline") or {}
                ).get("libraries") or []
    except Exception:
        return None
    for lib in libs:
        path = (lib.get("notebook") or {}).get("path") or ""
        if path.endswith(relative_path):
            return path
    return None
