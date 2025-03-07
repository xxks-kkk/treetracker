"""
Generate Postgres plans for JOB, SSB, TPC-H in JSON format
"""
import subprocess
from pathlib import Path

from jinja2 import Environment, BaseLoader

from icde2025.sqlite_perf import SSB_QUERY_DIR

PROJECT_DIR = Path.home() / "projects" / "treetracker2"

JOB_QUERY_DIR = PROJECT_DIR / "third-party" / "join-order-benchmark-postgres"
JOB_QUERY_DIR_TEST = PROJECT_DIR / "third-party" / "join-order-benchmark-postgres-test"
JOB_POSTGRES_DB = "imdb"
JOB_POSTGRES_PLAN_DIR = PROJECT_DIR / "results" / "job" / "with_predicates" / "postgres_plans"

SSB_POSTGRES_PLAN_DIR = PROJECT_DIR / "results" / "ssb" / "postgres_plans"

TPCH_POSTGRES_PLAN_DIR = PROJECT_DIR / "results" / "tpch" / "with_predicates" / "postgres_plans"
TPCH_POSTGRES_QUERY_DIR = PROJECT_DIR / "third-party" / "tpc-h-postgres"


def _run(query_dir, database, plan_dir, disable_analyze=False):
    POSTGRES_COMMAND = "psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"EXPLAIN (ANALYZE, FORMAT JSON) $(cat {{ query }})\""
    if disable_analyze:
        POSTGRES_COMMAND = "psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"EXPLAIN (FORMAT JSON) $(cat {{ query }})\""
    for query in list(Path(query_dir).glob('*.sql')):
        print(f"processing {query} ...")
        template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
        command = template.render(db=database, query=query)
        print(f"executing {command}")
        output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
        print(f"output: {output}")
        raw_plan = Path(query).stem + ".raw"
        file = plan_dir / raw_plan
        with open(file.as_posix(), mode="wt") as f:
            f.write(output)


def run_job():
    _run(JOB_QUERY_DIR, JOB_POSTGRES_DB, JOB_POSTGRES_PLAN_DIR)


def run_ssb():
    _run(SSB_QUERY_DIR, "ssb", SSB_POSTGRES_PLAN_DIR)


def run_tpch():
    _run(TPCH_POSTGRES_QUERY_DIR, "tpch", TPCH_POSTGRES_PLAN_DIR, True)


def process_to_json(plan_dir, ignore_list=None):
    for query in list(Path(plan_dir).glob('*.raw')):
        json = ""
        with open(query.as_posix(), mode="rt") as f:
            for line in f:
                if "Timing is on" not in line and \
                        "QUERY PLAN" not in line and \
                        "---" not in line and \
                        "(1 row)" not in line and \
                        "Time:" not in line and \
                        "SET" not in line:
                    if "+" in line:
                        json += line[:-2] + "\n"
                    else:
                        json += line
        if ignore_list is not None and Path(query).stem in ignore_list:
            filename = Path(query).stem + ".json.ignore"
        else:
            filename = Path(query).stem + ".json"
        output = plan_dir / filename
        with open(output.as_posix(), mode="wt") as f:
            f.write(json)


if __name__ == "__main__":
    # run_job()
    # process_to_json(JOB_POSTGRES_PLAN_DIR)
    # run_ssb()
    # process_to_json(SSB_POSTGRES_PLAN_DIR)
    run_tpch()
    # We ignore 15W, 20W because we don't want to handle them automatically in the downstream *FindOptJoinTree.java code gen.
    # We plan to incorporate them manually.
    process_to_json(TPCH_POSTGRES_PLAN_DIR, ["15W", "18W", "20W"])
