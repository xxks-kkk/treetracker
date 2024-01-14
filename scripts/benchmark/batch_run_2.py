import os
import re
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List

from benchmark.conf import BENCHMARK_NAME, MVN_BLD_CMD, SRC_DIR, \
    JAVA_EXECUTION_COMMANDS_FILE, BENCHMARK_TO_RUN, LOG_DIR, \
    gather_additional_stats_bench, ALGO_TO_RUN, Algorithm
from plot.utility import check_argument


def get_key_or_none(benchmark_config: dict, key: str):
    if key in benchmark_config:
        return benchmark_config[key]
    return None


def find_pattern_in_command(search_pattern: str, line: str) -> bool:
    if re.search(search_pattern, line) is not None:
        return True
    return False


def populate_benchmark_cmds(fpath: Path, search_patterns: List[str]) -> List:
    """
    Populate benchmark_cmds dictionary from fpath. Each line of fpath
    corresponds to a Java execution command of one benchmark.
    """
    benchmark_cmd = []
    with open(fpath, 'r', encoding='UTF-8') as file:
        while line := file.readline().rstrip():
            for search_pattern in search_patterns:
                if find_pattern_in_command(search_pattern, line):
                    benchmark_cmd.append(line)
                    print("find " + search_pattern)
                    break
    check_argument(len(benchmark_cmd) == len(search_patterns), f"len(benchmark_cmd): {len(benchmark_cmd)}"
                                                               f"\nlen(search_patterns): {len(search_patterns)}")
    return benchmark_cmd


def driver(benchmark_config: dict):
    """
    Carry out the benchmark.
    """
    def generate_search_patterns(benchmark_to_run: list, algorithms: List[Algorithm]):
        search_patterns = []
        for query_to_run in benchmark_to_run:
            for algorithm in algorithms:
                search_patterns.append(f"\\b{query_to_run}\\b.\\b{algorithm.value}\\b")
        return search_patterns

    log_file = Path(benchmark_config[LOG_DIR]).joinpath(
        benchmark_config[BENCHMARK_NAME] + "_log" + str(datetime.now().isoformat()) + ".txt")
    print("running bench: " + benchmark_config[BENCHMARK_NAME])

    search_patterns = generate_search_patterns(benchmark_config[BENCHMARK_TO_RUN], benchmark_config[ALGO_TO_RUN])
    benchmark_cmd = populate_benchmark_cmds(Path(benchmark_config[JAVA_EXECUTION_COMMANDS_FILE]),
                                            search_patterns)
    timeout_s = 86400
    with open(log_file, "a") as log:
        subprocess.call("exec " + MVN_BLD_CMD, cwd=SRC_DIR, stdout=log, shell=True)
        for java_cmd in benchmark_cmd:
            subprocess.call("exec " + java_cmd,
                            env=dict(os.environ, **{"treetracker_agg_stats_loc": Path(benchmark_config[LOG_DIR]).joinpath("additional_stats_job.csv")}),
                            stdout=log,
                            shell=True,
                            timeout=timeout_s)


if __name__ == "__main__":
    driver(benchmark_config=gather_additional_stats_bench)