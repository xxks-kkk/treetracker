"""
Run benchmark suite.
"""
import re
import signal
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List

from benchmark.conf import job_bench, BENCHMARK_NAME, MVN_BLD_CMD, SRC_DIR, \
    JAVA_EXECUTION_COMMANDS_FILE, SUPPORT_BENCHMARK_QUERIES, BENCHMARK_TO_RUN, LOG_DIR, \
    find_worst_case_join_ordering_bench, JOB, FIND_WORST_CASE_JOIN_ORDERING, pattern_bench, BENCHMARK_PATTERNS


class GracefulKiller:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *args):
        self.kill_now = True


def find_pattern_in_command(benchmark_id: str, support_query: str, line: str) -> bool:
    benchmark = "Benchmark"
    find_worst_case_join_ordering = "FindWorstCaseJoinOrdering"
    if benchmark_id == JOB:
        search_pattern = f"\\b{benchmark}{support_query}\\b"
    elif benchmark_id == FIND_WORST_CASE_JOIN_ORDERING:
        search_pattern = f"\\b{support_query}{find_worst_case_join_ordering}\\b"
    elif benchmark_id == BENCHMARK_PATTERNS:
        search_pattern = f"\\b{support_query}\\b"
    if re.search(search_pattern, line) is not None:
        return True
    return False


def populate_benchmark_cmds(fpath: Path, support_benchmark_queries: List[str], benchmark_id: str) -> dict:
    """
    Populate benchmark_cmds dictionary from fpath. Each line of fpath
    corresponds to a Java execution command of one benchmark.
    """
    benchmark_cmd = {}
    with open(fpath, 'r', encoding='UTF-8') as file:
        while line := file.readline().rstrip():
            for support_query in support_benchmark_queries:
                support_query_found = False
                if find_pattern_in_command(benchmark_id, support_query, line):
                    benchmark_cmd[support_query] = line
                    support_query_found = True
                    break
            if not support_query_found:
                raise RuntimeError(f"The following line doesn't correspond to any support queries. Support queries:"
                                   f"{support_benchmark_queries}\n line: {line}")
            else:
                support_query_found = False
    return benchmark_cmd


def driver(benchmark_config: dict):
    """
    Carry out the benchmark.
    """
    log_file = Path(benchmark_config[LOG_DIR]).joinpath(
        benchmark_config[BENCHMARK_NAME] + "_log" + str(datetime.now().isoformat()) + ".txt")
    benchmark_cmd = populate_benchmark_cmds(Path(benchmark_config[JAVA_EXECUTION_COMMANDS_FILE]),
                                            benchmark_config[SUPPORT_BENCHMARK_QUERIES],
                                            benchmark_config[BENCHMARK_NAME])
    timeout_s = 86400
    with open(log_file, "a") as log:
        subprocess.call("exec " + MVN_BLD_CMD, cwd=SRC_DIR, stdout=log, shell=True)
        # handle term signal and wait. A combination solution of:
        # - https://stackoverflow.com/a/13143013/1460102
        # - https://stackoverflow.com/a/34459144/1460102
        for query_to_run in benchmark_config[BENCHMARK_TO_RUN]:
            subprocess.call("exec " + benchmark_cmd[query_to_run], stdout=log, shell=True, timeout=timeout_s)


if __name__ == "__main__":
    driver(benchmark_config=job_bench)
    driver(benchmark_config=find_worst_case_join_ordering_bench)
    driver(benchmark_config=pattern_bench)