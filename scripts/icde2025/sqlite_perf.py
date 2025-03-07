"""
Measure the benchmark performance on SQLite
"""
import subprocess
from pathlib import Path

from jinja2 import Environment, BaseLoader

PROJECT_DIR=Path.home() / "projects" / "treetracker2"

JOB_QUERY_DIR=PROJECT_DIR / "third-party" / "join-order-benchmark"
JOB_SQLITE_DB=PROJECT_DIR / "imdb.sqlitedb"
JOB_SQLITE_PERFORMANCE_FILE=PROJECT_DIR / "results" / "job" / "with_predicates" / "sqlite_perf" / "raw.txt"

TPCH_QUERY_DIR=PROJECT_DIR / "third-party" / "tpc-h-without-explain"
TPCH_SQLITE_DB=PROJECT_DIR / "tpch.sqlitedb"
TPCH_SQLITE_PERFORMANCE_FILE=PROJECT_DIR / "results" / "tpch" / "with_predicates" / "sqlite_perf" / "raw.txt"

SSB_QUERY_DIR=PROJECT_DIR / "third-party" / "star-schema-benchmark"
SSB_SQLITE_DB=PROJECT_DIR / "ssb.sqlitedb"
SSB_SQLITE_PERFORMANCE_FILE=PROJECT_DIR / "results" / "ssb" / "sqlite_perf" / "raw.txt"

def run_job():
    SQLITE_COMMAND="sqlite3 -cmd \".timer on\" {{ db }} < {{ query }}"
    with open(JOB_SQLITE_PERFORMANCE_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(JOB_QUERY_DIR).glob('*.sql')):
            print(f"processing {query} ...")
            template = Environment(loader=BaseLoader()).from_string(SQLITE_COMMAND)
            command = template.render(db=JOB_SQLITE_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")

def process_job_raw_data():
    def _extract_query_from_line(line):
        path = Path(line)
        filename_without_extension = path.stem
        return f"query{filename_without_extension}"

    def _extract_runtime_from_line(line):
        """
        Assume `Run Time: real 0.078 user 0.065536 sys 0.011819`.
        Since the time is in seconds, we return milliseconds, e.g., 78
        """
        parts = line.split()
        return float(parts[3]) * 1000


    result=dict()
    with open(JOB_SQLITE_PERFORMANCE_FILE.as_posix(), mode="r") as f:
        query = ""
        runtime_count = 0
        runtime = 0
        for line in f:
            if 'join-order-benchmark' in line:
                query = _extract_query_from_line(line)
            if 'Run Time' in line:
                runtime_count += 1
                runtime += _extract_runtime_from_line(line)
                if runtime_count == 2:
                    result[query] = runtime
                    runtime_count = 0
                    runtime = 0
    return result

def run_tpch():
    SQLITE_COMMAND="sqlite3 -cmd \".timer on\" {{ db }} < {{ query }}"
    with open(TPCH_SQLITE_PERFORMANCE_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(TPCH_QUERY_DIR).glob('*.sql')):
            print(f"processing {query} ...")
            template = Environment(loader=BaseLoader()).from_string(SQLITE_COMMAND)
            command = template.render(db=TPCH_SQLITE_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")


def process_tpch_raw_data():
    def _extract_query_from_line(line):
        path = Path(line)
        filename_without_extension = path.stem
        return f"query{filename_without_extension}"

    def _extract_runtime_from_line(line):
        """
        Assume `Run Time: real 0.078 user 0.065536 sys 0.011819`.
        Since the time is in seconds, we return milliseconds, e.g., 78
        """
        parts = line.split()
        return float(parts[3]) * 1000

    result=dict()
    with open(TPCH_SQLITE_PERFORMANCE_FILE.as_posix(), mode="r") as f:
        query = ""
        q15w_runtime_count = 0
        q15w_runtime = 0
        for line in f:
            if 'tpc-h-without-explain' in line:
                query = _extract_query_from_line(line)
            if 'Run Time' in line:
                if query != 'query15W':
                    result[query] = _extract_runtime_from_line(line)
                else:
                    q15w_runtime_count += 1
                    q15w_runtime += _extract_runtime_from_line(line)
                    if q15w_runtime_count == 2:
                        result[query] = q15w_runtime
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

def run_ssb():
    SQLITE_COMMAND="sqlite3 -cmd \".timer on\" {{ db }} < {{ query }}"
    with open(SSB_SQLITE_PERFORMANCE_FILE.as_posix(), mode="wt") as f:
        for query in list(Path(SSB_QUERY_DIR).glob('*.sql')):
            print(f"processing {query} ...")
            template = Environment(loader=BaseLoader()).from_string(SQLITE_COMMAND)
            command = template.render(db=SSB_SQLITE_DB, query=query)
            print(f"executing {command}")
            output = subprocess.check_output(command, shell=True, cwd=PROJECT_DIR.as_posix(), text=True)
            print(f"output: {output}")
            f.write(f"{query}\n")
            f.write(output)
            f.write("--\n")

def process_ssb_raw_data():
    def _extract_query_from_line(line):
        path = Path(line)
        filename_without_extension = path.stem
        return f"query{filename_without_extension}"

    def _extract_runtime_from_line(line):
        """
        Assume `Run Time: real 0.078 user 0.065536 sys 0.011819`.
        Since the time is in seconds, we return milliseconds, e.g., 78
        """
        parts = line.split()
        return float(parts[3]) * 1000

    result=dict()
    with open(SSB_SQLITE_PERFORMANCE_FILE.as_posix(), mode="r") as f:
        query = ""
        q15w_runtime_count = 0
        q15w_runtime = 0
        for line in f:
            if 'star-schema-benchmark' in line:
                query = _extract_query_from_line(line)
            if 'Run Time' in line:
                result[query] = _extract_runtime_from_line(line)
    return_result = []
    labels = ["query1P1", "query1P2", "query1P3", "query2P1", "query2P2", "query2P3", "query3P1", "query3P2",
              "query3P3", "query3P4", "query4P1", "query4P2", "query4P3"]
    for label in labels:
        return_result.append(result[label])
    return return_result




if __name__ == "__main__":
    # run_job()
    # process_job_raw_data()
    run_tpch()
    # process_tpch_raw_data()
    # run_ssb()
    # process_ssb_raw_data()
