"""
Measure the JOB benchmark performance on postgres

NOTE: you need to run this script using `sudo` because we need to clear both OS cache and buffer cache (see job-postgres.md.html).
Here are the steps to run this script:
1. $sudo -s
2. $cd /home/zeyuanhu/projects/treetracker2
3. $source scripts/env/bin/activate
4. $python3 scripts/icde2025/postgres_perf.py
5. $exit
"""
import subprocess
from pathlib import Path

from jinja2 import Environment, BaseLoader

# Note to draw the plot, we need to replace `Path("/home/zeyuanhu")` with `Path.home()`.
# The reason we hardcode `Path("/home/zeyuanhu")` because we run this script in sudo and `Path.home()` no longer points
# to "/home/zeyuanhu" once in sudo.
PROJECT_DIR = Path("/home/zeyuanhu") / "projects" / "treetracker2"

JOB_QUERY_DIR = PROJECT_DIR / "third-party" / "join-order-benchmark-postgres"
JOB_QUERY_DIR_TEST = PROJECT_DIR / "third-party" / "join-order-benchmark-postgres-test"
JOB_QUERY_DIR_ENFORCED = PROJECT_DIR / "third-party" / "join-order-benchmark-postgres-enforced"
JOB_POSTGRES_DB = "imdb"
CLEAR_CACHE_FILE = PROJECT_DIR / "scripts" / "icde2025" / "clear-cache.sh"
JOB_POSTGRES_PERFORMANCE_FILE = PROJECT_DIR / "results" / "job" / "with_predicates" / "postgres_perf" / "raw.txt"
JOB_POSTGRES_PERFORMANCE_ENFORCED_FILE = PROJECT_DIR / "results" / "job" / "with_predicates" / "postgres_perf" / "raw_enforced.txt"
JOB_POSTGRES_PERFORMANCE_ENFORCED_DEBUG_FILE = PROJECT_DIR / "results" / "job" / "with_predicates" / "postgres_perf" / "raw_enforced_debug.txt"

TPCH_QUERY_DIR_ENFORCED = PROJECT_DIR / "third-party" / "tpc-h-postgres-enforced"
TPCH_POSTGRES_DB = "tpch"
TPCH_POSTGRES_PERFORMANCE_ENFORCED_FILE = PROJECT_DIR / "results" / "tpch" / "with_predicates" / "postgres_perf" / "raw_enforced.txt"
TPCH_POSTGRES_PERFORMANCE_ENFORCED_DEBUG_FILE = PROJECT_DIR / "results" / "tpch" / "with_predicates" / "postgres_perf" / "raw_enforced_debug.txt"
TPCH_POSTGRES_PERFORMANCE_FILE = PROJECT_DIR / "results" / "tpch" / "with_predicates" / "postgres_perf" / "raw.txt"
TPCH_POSTGRES_QUERY_DIR = PROJECT_DIR / "third-party" / "tpc-h-postgres"

SSB_QUERY_DIR = PROJECT_DIR / "third-party" / "star-schema-benchmark"
SSB_QUERY_DIR_ENFORCED = PROJECT_DIR / "third-party" / "star-schema-benchmark-postgres-enforced"
SSB_POSTGRES_DB = "ssb"
SSB_POSTGRES_PERFORMANCE_FILE = PROJECT_DIR / "results" / "ssb" / "postgres_perf" / "raw.txt"
SSB_POSTGRES_PERFORMANCE_ENFORCED_FILE = PROJECT_DIR / "results" / "ssb" / "postgres_perf" / "raw_enforced.txt"
SSB_POSTGRES_PERFORMANCE_ENFORCED_DEBUG_FILE = PROJECT_DIR / "results" / "ssb" / "postgres_perf" / "raw_enforced_debug.txt"


def run_job():
    CLEARCACHE_COMMAND = "bash {{ clear_cache_file }}"
    POSTGRES_COMMAND = "sudo -u postgres psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c '\\timing' -c \"$(cat {{ query }})\""
    with open(JOB_POSTGRES_PERFORMANCE_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(JOB_QUERY_DIR).glob('*.sql')):
            print(f"processing {query} ...")
            clear_cache_template = Environment(loader=BaseLoader()).from_string(CLEARCACHE_COMMAND)
            clear_cache_command = clear_cache_template.render(clear_cache_file=CLEAR_CACHE_FILE)
            print(f"executing {clear_cache_command}")
            subprocess.check_output(clear_cache_command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=JOB_POSTGRES_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")


def run_job_enforced_debug():
    """
    Running JOB queries with pg_hint_plan. This is for debug to ensure all the hints are successfully applied.
    Note that we assume pg_hint_plan is installed.
    """
    POSTGRES_COMMAND = "psql -d imdb -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"SET pg_hint_plan.enable_hint=ON\" -c \"SET pg_hint_plan.debug_print TO on\" -c \"SET pg_hint_plan.debug_print=verbose\" -c \"SET client_min_messages = log\" -c \"EXPLAIN $(cat {{ query }})\""
    num_queries_hints_not_applied = 0
    with open(JOB_POSTGRES_PERFORMANCE_ENFORCED_DEBUG_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(JOB_QUERY_DIR_ENFORCED).glob('*.sql')):
            print(f"processing {query} ...")
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=POSTGRES_COMMAND, query=query)
            print(f"executing {command}")
            output = subprocess.run(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True, capture_output=True,
                                    check=True)
            f.write(f"{query}\n")
            # print(f"output.stderr: {output.stderr}")
            f.write(output.stderr)
            f.write(output.stdout)
            f.write("--\n")
            if "{used hints:(none)}" in output.stderr:
                print(f"Hints are not applied: {query}")
                num_queries_hints_not_applied += 1
    print(f"{num_queries_hints_not_applied} queries not applied hints")


def run_job_enforced():
    """
    Running JOB queries with pg_hint_plan. Note that we assume pg_hint_plan is installed.
    """
    CLEARCACHE_COMMAND = "bash {{ clear_cache_file }}"
    POSTGRES_COMMAND = "sudo -u postgres psql -d imdb -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"SET pg_hint_plan.enable_hint=ON\" -c '\\timing' -c \"$(cat {{ query }})\""
    with open(JOB_POSTGRES_PERFORMANCE_ENFORCED_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(JOB_QUERY_DIR_ENFORCED).glob('*.sql')):
            print(f"processing {query} ...")
            clear_cache_template = Environment(loader=BaseLoader()).from_string(CLEARCACHE_COMMAND)
            clear_cache_command = clear_cache_template.render(clear_cache_file=CLEAR_CACHE_FILE)
            print(f"executing {clear_cache_command}")
            subprocess.check_output(clear_cache_command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=POSTGRES_COMMAND, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")


def process_job_raw_data():
    return _process_job_raw_data(JOB_POSTGRES_PERFORMANCE_FILE)


def process_job_enforced_raw_data():
    return _process_job_raw_data(JOB_POSTGRES_PERFORMANCE_ENFORCED_FILE)


def _process_job_raw_data(performance_file):
    def _extract_query_from_line(line):
        path = Path(line)
        filename_without_extension = path.stem
        return f"query{filename_without_extension}"

    def _extract_runtime_from_line(line):
        """
        Assume `Time: 2700.334 ms (00:02.700)`
        """
        parts = line.split()
        return float(parts[1])

    result = dict()
    with open(performance_file.as_posix(), mode="r") as f:
        query = ""
        runtime_count = 0
        runtime = 0
        for line in f:
            if 'join-order-benchmark' in line:
                query = _extract_query_from_line(line)
            if 'Time' in line and 'Time Champion' not in line:
                runtime_count += 1
                runtime += _extract_runtime_from_line(line)
                if runtime_count == 1:
                    result[query] = runtime
                    runtime_count = 0
                    runtime = 0
    return result


def run_tpch_enforced_debug():
    """
    Running TPC-H queries with pg_hint_plan. This is for debug to ensure all the hints are successfully applied.
    Note that we assume pg_hint_plan is installed.
    """
    POSTGRES_COMMAND = "psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"SET pg_hint_plan.enable_hint=ON\" -c \"SET pg_hint_plan.debug_print TO on\" -c \"SET pg_hint_plan.debug_print=verbose\" -c \"SET client_min_messages = log\" -c \"EXPLAIN $(cat {{ query }})\""
    num_queries_hints_not_applied = 0
    with open(TPCH_POSTGRES_PERFORMANCE_ENFORCED_DEBUG_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(TPCH_QUERY_DIR_ENFORCED).glob('*.sql')):
            print(f"processing {query} ...")
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=TPCH_POSTGRES_DB, query=query)
            print(f"executing {command}")
            output = subprocess.run(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True, capture_output=True,
                                    check=True)
            f.write(f"{query}\n")
            # print(f"output.stderr: {output.stderr}")
            f.write(output.stderr)
            f.write(output.stdout)
            f.write("--\n")
            if "{used hints:(none)}" in output.stderr:
                print(f"Hints are not applied: {query}")
                num_queries_hints_not_applied += 1
    print(f"{num_queries_hints_not_applied} queries not applied hints")


def run_tpch_enforced():
    """
    Running TPC-H queries with pg_hint_plan. Note that we assume pg_hint_plan is installed.
    """
    CLEARCACHE_COMMAND = "bash {{ clear_cache_file }}"
    POSTGRES_COMMAND = "sudo -u postgres psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"SET pg_hint_plan.enable_hint=ON\" -c '\\timing' -c \"$(cat {{ query }})\""
    with open(TPCH_POSTGRES_PERFORMANCE_ENFORCED_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(TPCH_QUERY_DIR_ENFORCED).glob('*.sql')):
            print(f"processing {query} ...")
            clear_cache_template = Environment(loader=BaseLoader()).from_string(CLEARCACHE_COMMAND)
            clear_cache_command = clear_cache_template.render(clear_cache_file=CLEAR_CACHE_FILE)
            print(f"executing {clear_cache_command}")
            subprocess.check_output(clear_cache_command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=TPCH_POSTGRES_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")


def run_ssb_enforced_debug():
    """
    Running SSB queries with pg_hint_plan. This is for debug to ensure all the hints are successfully applied.
    Note that we assume pg_hint_plan is installed.
    """
    POSTGRES_COMMAND = "psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"SET pg_hint_plan.enable_hint=ON\" -c \"SET pg_hint_plan.debug_print TO on\" -c \"SET pg_hint_plan.debug_print=verbose\" -c \"SET client_min_messages = log\" -c \"EXPLAIN $(cat {{ query }})\""
    num_queries_hints_not_applied = 0
    with open(SSB_POSTGRES_PERFORMANCE_ENFORCED_DEBUG_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(SSB_QUERY_DIR_ENFORCED).glob('*.sql')):
            print(f"processing {query} ...")
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=SSB_POSTGRES_DB, query=query)
            print(f"executing {command}")
            output = subprocess.run(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True, capture_output=True,
                                    check=True)
            f.write(f"{query}\n")
            # print(f"output.stderr: {output.stderr}")
            f.write(output.stderr)
            f.write(output.stdout)
            f.write("--\n")
            if "{used hints:(none)}" in output.stderr:
                print(f"Hints are not applied: {query}")
                num_queries_hints_not_applied += 1
    print(f"{num_queries_hints_not_applied} queries not applied hints")


def run_tpch():
    """
    Run TPC-H queries with native Postgres plans
    """
    CLEARCACHE_COMMAND = "bash {{ clear_cache_file }}"
    POSTGRES_COMMAND = "sudo -u postgres psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c '\\timing' -c \"$(cat {{ query }})\""
    with open(TPCH_POSTGRES_PERFORMANCE_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(TPCH_POSTGRES_QUERY_DIR).glob('*.sql')):
            print(f"processing {query} ...")
            if "20W" in query.stem:
                continue
            clear_cache_template = Environment(loader=BaseLoader()).from_string(CLEARCACHE_COMMAND)
            clear_cache_command = clear_cache_template.render(clear_cache_file=CLEAR_CACHE_FILE)
            print(f"executing {clear_cache_command}")
            subprocess.check_output(clear_cache_command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=TPCH_POSTGRES_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")


def run_ssb_enforced():
    """
    Running SSB queries with pg_hint_plan.
    """
    CLEARCACHE_COMMAND = "bash {{ clear_cache_file }}"
    POSTGRES_COMMAND = "sudo -u postgres psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c \"SET pg_hint_plan.enable_hint=ON\" -c '\\timing' -c \"$(cat {{ query }})\""
    with open(SSB_POSTGRES_PERFORMANCE_ENFORCED_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(SSB_QUERY_DIR_ENFORCED).glob('*.sql')):
            print(f"processing {query} ...")
            clear_cache_template = Environment(loader=BaseLoader()).from_string(CLEARCACHE_COMMAND)
            clear_cache_command = clear_cache_template.render(clear_cache_file=CLEAR_CACHE_FILE)
            print(f"executing {clear_cache_command}")
            subprocess.check_output(clear_cache_command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=SSB_POSTGRES_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")


def run_ssb():
    """
    Running SSB queries with native Postgres plans
    """
    CLEARCACHE_COMMAND = "bash {{ clear_cache_file }}"
    POSTGRES_COMMAND = "sudo -u postgres psql -d {{ db }} -c \"SET ENABLE_NESTLOOP TO FALSE\" -c \"set enable_mergejoin=off\" -c '\\timing' -c \"$(cat {{ query }})\""
    with open(SSB_POSTGRES_PERFORMANCE_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(SSB_QUERY_DIR).glob('*.sql')):
            print(f"processing {query} ...")
            clear_cache_template = Environment(loader=BaseLoader()).from_string(CLEARCACHE_COMMAND)
            clear_cache_command = clear_cache_template.render(clear_cache_file=CLEAR_CACHE_FILE)
            print(f"executing {clear_cache_command}")
            subprocess.check_output(clear_cache_command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            template = Environment(loader=BaseLoader()).from_string(POSTGRES_COMMAND)
            command = template.render(db=SSB_POSTGRES_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")


def _process_tpch_raw_data(performance_file):
    # Per discusssion with Remy, due to Postgres 20W uses two nested-loop semijoins, it takes
    # two days to run without finish. Thus, we use timeout with value set to 1 minute or 60000 ms.
    timeout = 60 * 1000

    def _extract_query_from_line(line):
        path = Path(line)
        filename_without_extension = path.stem
        return f"query{filename_without_extension}"

    def _extract_runtime_from_line(line):
        """
        Assume `Time: 2700.334 ms (00:02.700)`
        """
        parts = line.split()
        return float(parts[1])

    result = dict()
    with open(performance_file.as_posix(), mode="r") as f:
        query = ""
        for line in f:
            if 'tpc-h-postgres' in line:
                query = _extract_query_from_line(line)
            if 'Time' in line:
                result[query] = _extract_runtime_from_line(line)

    result["query20W"] = timeout
    return_result = []
    labels = ["query3W", "query7aW", "query7bW", "query8W", "query9W", "query10W", "query11W",
              "query12W", "query14W", "query15W", "query16W", "query18W",
              "query19aW", "query19bW", "query19cW", "query20W"]
    q7w_runtime = 0
    q19w_runtime = 0
    for label in labels:
        if label == "query7aW":
            q7w_runtime += result[label]
        elif label == "query7bW":
            q7w_runtime += result[label]
            return_result.append(q7w_runtime)
        elif label == "query19aW":
            q19w_runtime += result[label]
        elif label == "query19bW":
            q19w_runtime += result[label]
        elif label == "query19cW":
            q19w_runtime += result[label]
            return_result.append(q19w_runtime)
        else:
            return_result.append(result[label])
    return return_result


def process_tpch_enforced_raw_data():
    return _process_tpch_raw_data(TPCH_POSTGRES_PERFORMANCE_ENFORCED_FILE)


def process_tpch_raw_data():
    return _process_tpch_raw_data(TPCH_POSTGRES_PERFORMANCE_FILE)


def _process_ssb_raw_data(performance_file):
    def _extract_query_from_line(line):
        path = Path(line)
        filename_without_extension = path.stem
        return f"{filename_without_extension}"

    def _extract_runtime_from_line(line):
        """
        Assume `Time: 2700.334 ms (00:02.700)`
        """
        parts = line.split()
        return float(parts[1])

    result = dict()
    with open(performance_file.as_posix(), mode="r") as f:
        query = ""
        runtime_count = 0
        runtime = 0
        for line in f:
            if 'star-schema-benchmark' in line:
                query = _extract_query_from_line(line)
            if 'Time' in line and 'Time Champion' not in line:
                runtime_count += 1
                runtime += _extract_runtime_from_line(line)
                if runtime_count == 1:
                    result[query] = runtime
                    runtime_count = 0
                    runtime = 0
    return_result = []
    labels = ["1P1", "1P2", "1P3", "2P1", "2P2", "2P3", "3P1", "3P2", "3P3", "3P4", "4P1", "4P2", "4P3"]
    for label in labels:
        return_result.append(result[label])
    return return_result


def process_ssb_enforced_raw_data():
    return _process_ssb_raw_data(SSB_POSTGRES_PERFORMANCE_ENFORCED_FILE)


def process_ssb_raw_data():
    return _process_ssb_raw_data(SSB_POSTGRES_PERFORMANCE_FILE)


if __name__ == "__main__":
    # run_job()
    # process_job_raw_data()
    # run_job_enforced_debug()
    # run_job_enforced()
    # process_job_enforced_raw_data()
    # run_tpch_enforced_debug()
    # run_tpch_enforced()
    # process_tpch_enforced_raw_data()
    # run_ssb_enforced_debug()
    # run_ssb_enforced()
    # process_ssb_enforced_raw_data()
    # run_ssb()
    # process_ssb_raw_data()
    # run_tpch()
    process_tpch_raw_data()