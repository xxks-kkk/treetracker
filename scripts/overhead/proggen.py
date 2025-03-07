"""
Generate java file based on `ReductionTimeProfiling.javat` for profiling specific algorithm on specific benchmark
"""
import re
from pathlib import Path

import jinja2
from jinja2 import FileSystemLoader


class Benchmark:
    job = "job"


class Algorithm:
    TTJ = "TTJHP"

RUNTIMES=50

class TTJHPJOB:
    Query10aOptJoinTreeOptOrderingShallowHJOrdering = "Query10aOptJoinTreeOptOrderingShallowHJOrdering"
    Query10bOptJoinTreeOptOrderingShallowHJOrdering = "Query10bOptJoinTreeOptOrderingShallowHJOrdering"
    Query10cOptJoinTreeOptOrderingShallowHJOrdering = "Query10cOptJoinTreeOptOrderingShallowHJOrdering"
    Query11aOptJoinTreeOptOrderingShallowHJOrdering = "Query11aOptJoinTreeOptOrderingShallowHJOrdering"
    Query11bOptJoinTreeOptOrderingShallowHJOrdering = "Query11bOptJoinTreeOptOrderingShallowHJOrdering"
    Query11cOptJoinTreeOptOrderingShallowHJOrdering = "Query11cOptJoinTreeOptOrderingShallowHJOrdering"
    Query11dOptJoinTreeOptOrderingShallowHJOrdering = "Query11dOptJoinTreeOptOrderingShallowHJOrdering"
    Query12aOptJoinTreeOptOrderingShallowHJOrdering = "Query12aOptJoinTreeOptOrderingShallowHJOrdering"
    Query12bOptJoinTreeOptOrderingShallowHJOrdering = "Query12bOptJoinTreeOptOrderingShallowHJOrdering"
    Query12cOptJoinTreeOptOrderingShallowHJOrdering = "Query12cOptJoinTreeOptOrderingShallowHJOrdering"
    Query13aOptJoinTreeOptOrderingShallowHJOrdering = "Query13aOptJoinTreeOptOrderingShallowHJOrdering"
    Query13bOptJoinTreeOptOrderingShallowHJOrdering = "Query13bOptJoinTreeOptOrderingShallowHJOrdering"
    Query13cOptJoinTreeOptOrderingShallowHJOrdering = "Query13cOptJoinTreeOptOrderingShallowHJOrdering"
    Query13dOptJoinTreeOptOrderingShallowHJOrdering = "Query13dOptJoinTreeOptOrderingShallowHJOrdering"
    Query14aOptJoinTreeOptOrderingShallowHJOrdering = "Query14aOptJoinTreeOptOrderingShallowHJOrdering"
    Query14bOptJoinTreeOptOrderingShallowHJOrdering = "Query14bOptJoinTreeOptOrderingShallowHJOrdering"
    Query14cOptJoinTreeOptOrderingShallowHJOrdering = "Query14cOptJoinTreeOptOrderingShallowHJOrdering"
    Query15aOptJoinTreeOptOrderingShallowHJOrdering = "Query15aOptJoinTreeOptOrderingShallowHJOrdering"
    Query15bOptJoinTreeOptOrderingShallowHJOrdering = "Query15bOptJoinTreeOptOrderingShallowHJOrdering"
    Query15cOptJoinTreeOptOrderingShallowHJOrdering = "Query15cOptJoinTreeOptOrderingShallowHJOrdering"
    Query15dOptJoinTreeOptOrderingShallowHJOrdering = "Query15dOptJoinTreeOptOrderingShallowHJOrdering"
    Query16aOptJoinTreeOptOrderingShallowHJOrdering = "Query16aOptJoinTreeOptOrderingShallowHJOrdering"
    Query16bOptJoinTreeOptOrderingShallowHJOrdering = "Query16bOptJoinTreeOptOrderingShallowHJOrdering"
    Query16cOptJoinTreeOptOrderingShallowHJOrdering = "Query16cOptJoinTreeOptOrderingShallowHJOrdering"
    Query16dOptJoinTreeOptOrderingShallowHJOrdering = "Query16dOptJoinTreeOptOrderingShallowHJOrdering"
    Query17aOptJoinTreeOptOrderingShallowHJOrdering = "Query17aOptJoinTreeOptOrderingShallowHJOrdering"
    Query17bOptJoinTreeOptOrderingShallowHJOrdering = "Query17bOptJoinTreeOptOrderingShallowHJOrdering"
    Query17cOptJoinTreeOptOrderingShallowHJOrdering = "Query17cOptJoinTreeOptOrderingShallowHJOrdering"
    Query17dOptJoinTreeOptOrderingShallowHJOrdering = "Query17dOptJoinTreeOptOrderingShallowHJOrdering"
    Query17eOptJoinTreeOptOrderingShallowHJOrdering = "Query17eOptJoinTreeOptOrderingShallowHJOrdering"
    Query17fOptJoinTreeOptOrderingShallowHJOrdering = "Query17fOptJoinTreeOptOrderingShallowHJOrdering"
    Query18aOptJoinTreeOptOrderingShallowHJOrdering = "Query18aOptJoinTreeOptOrderingShallowHJOrdering"
    Query18bOptJoinTreeOptOrderingShallowHJOrdering = "Query18bOptJoinTreeOptOrderingShallowHJOrdering"
    Query18cOptJoinTreeOptOrderingShallowHJOrdering = "Query18cOptJoinTreeOptOrderingShallowHJOrdering"
    Query19aOptJoinTreeOptOrderingShallowHJOrdering = "Query19aOptJoinTreeOptOrderingShallowHJOrdering"
    Query19bOptJoinTreeOptOrderingShallowHJOrdering = "Query19bOptJoinTreeOptOrderingShallowHJOrdering"
    Query19cOptJoinTreeOptOrderingShallowHJOrdering = "Query19cOptJoinTreeOptOrderingShallowHJOrdering"
    Query19dOptJoinTreeOptOrderingShallowHJOrdering = "Query19dOptJoinTreeOptOrderingShallowHJOrdering"
    Query1aOptJoinTreeOptOrderingShallowHJOrdering = "Query1aOptJoinTreeOptOrderingShallowHJOrdering"
    Query1bOptJoinTreeOptOrderingShallowHJOrdering = "Query1bOptJoinTreeOptOrderingShallowHJOrdering"
    Query1cOptJoinTreeOptOrderingShallowHJOrdering = "Query1cOptJoinTreeOptOrderingShallowHJOrdering"
    Query1dOptJoinTreeOptOrderingShallowHJOrdering = "Query1dOptJoinTreeOptOrderingShallowHJOrdering"
    Query20aOptJoinTreeOptOrderingShallowHJOrdering = "Query20aOptJoinTreeOptOrderingShallowHJOrdering"
    Query20bOptJoinTreeOptOrderingShallowHJOrdering = "Query20bOptJoinTreeOptOrderingShallowHJOrdering"
    Query20cOptJoinTreeOptOrderingShallowHJOrdering = "Query20cOptJoinTreeOptOrderingShallowHJOrdering"
    Query21aOptJoinTreeOptOrderingShallowHJOrdering = "Query21aOptJoinTreeOptOrderingShallowHJOrdering"
    Query21bOptJoinTreeOptOrderingShallowHJOrdering = "Query21bOptJoinTreeOptOrderingShallowHJOrdering"
    Query21cOptJoinTreeOptOrderingShallowHJOrdering = "Query21cOptJoinTreeOptOrderingShallowHJOrdering"
    Query22aOptJoinTreeOptOrderingShallowHJOrdering = "Query22aOptJoinTreeOptOrderingShallowHJOrdering"
    Query22bOptJoinTreeOptOrderingShallowHJOrdering = "Query22bOptJoinTreeOptOrderingShallowHJOrdering"
    Query22cOptJoinTreeOptOrderingShallowHJOrdering = "Query22cOptJoinTreeOptOrderingShallowHJOrdering"
    Query22dOptJoinTreeOptOrderingShallowHJOrdering = "Query22dOptJoinTreeOptOrderingShallowHJOrdering"
    Query23aOptJoinTreeOptOrderingShallowHJOrdering = "Query23aOptJoinTreeOptOrderingShallowHJOrdering"
    Query23bOptJoinTreeOptOrderingShallowHJOrdering = "Query23bOptJoinTreeOptOrderingShallowHJOrdering"
    Query23cOptJoinTreeOptOrderingShallowHJOrdering = "Query23cOptJoinTreeOptOrderingShallowHJOrdering"
    Query24aOptJoinTreeOptOrderingShallowHJOrdering = "Query24aOptJoinTreeOptOrderingShallowHJOrdering"
    Query24bOptJoinTreeOptOrderingShallowHJOrdering = "Query24bOptJoinTreeOptOrderingShallowHJOrdering"
    Query25aOptJoinTreeOptOrderingShallowHJOrdering = "Query25aOptJoinTreeOptOrderingShallowHJOrdering"
    Query25bOptJoinTreeOptOrderingShallowHJOrdering = "Query25bOptJoinTreeOptOrderingShallowHJOrdering"
    Query25cOptJoinTreeOptOrderingShallowHJOrdering = "Query25cOptJoinTreeOptOrderingShallowHJOrdering"
    Query26aOptJoinTreeOptOrderingShallowHJOrdering = "Query26aOptJoinTreeOptOrderingShallowHJOrdering"
    Query26bOptJoinTreeOptOrderingShallowHJOrdering = "Query26bOptJoinTreeOptOrderingShallowHJOrdering"
    Query26cOptJoinTreeOptOrderingShallowHJOrdering = "Query26cOptJoinTreeOptOrderingShallowHJOrdering"
    Query27aOptJoinTreeOptOrderingShallowHJOrdering = "Query27aOptJoinTreeOptOrderingShallowHJOrdering"
    Query27bOptJoinTreeOptOrderingShallowHJOrdering = "Query27bOptJoinTreeOptOrderingShallowHJOrdering"
    Query27cOptJoinTreeOptOrderingShallowHJOrdering = "Query27cOptJoinTreeOptOrderingShallowHJOrdering"
    Query28aOptJoinTreeOptOrderingShallowHJOrdering = "Query28aOptJoinTreeOptOrderingShallowHJOrdering"
    Query28bOptJoinTreeOptOrderingShallowHJOrdering = "Query28bOptJoinTreeOptOrderingShallowHJOrdering"
    Query28cOptJoinTreeOptOrderingShallowHJOrdering = "Query28cOptJoinTreeOptOrderingShallowHJOrdering"
    Query29aOptJoinTreeOptOrderingShallowHJOrdering = "Query29aOptJoinTreeOptOrderingShallowHJOrdering"
    Query29bOptJoinTreeOptOrderingShallowHJOrdering = "Query29bOptJoinTreeOptOrderingShallowHJOrdering"
    Query29cOptJoinTreeOptOrderingShallowHJOrdering = "Query29cOptJoinTreeOptOrderingShallowHJOrdering"
    Query2aOptJoinTreeOptOrderingShallowHJOrdering = "Query2aOptJoinTreeOptOrderingShallowHJOrdering"
    Query2bOptJoinTreeOptOrderingShallowHJOrdering = "Query2bOptJoinTreeOptOrderingShallowHJOrdering"
    Query2cOptJoinTreeOptOrderingShallowHJOrdering = "Query2cOptJoinTreeOptOrderingShallowHJOrdering",
    Query2dOptJoinTreeOptOrderingShallowHJOrdering = "Query2dOptJoinTreeOptOrderingShallowHJOrdering"
    Query30aOptJoinTreeOptOrderingShallowHJOrdering = "Query30aOptJoinTreeOptOrderingShallowHJOrdering"
    Query30bOptJoinTreeOptOrderingShallowHJOrdering = "Query30bOptJoinTreeOptOrderingShallowHJOrdering"
    Query30cOptJoinTreeOptOrderingShallowHJOrdering = "Query30cOptJoinTreeOptOrderingShallowHJOrdering"
    Query31aOptJoinTreeOptOrderingShallowHJOrdering = "Query31aOptJoinTreeOptOrderingShallowHJOrdering"
    Query31bOptJoinTreeOptOrderingShallowHJOrdering =  "Query31bOptJoinTreeOptOrderingShallowHJOrdering"
    Query31cOptJoinTreeOptOrderingShallowHJOrdering = "Query31cOptJoinTreeOptOrderingShallowHJOrdering"
    Query32aOptJoinTreeOptOrderingShallowHJOrdering = "Query32aOptJoinTreeOptOrderingShallowHJOrdering"
    Query32bOptJoinTreeOptOrderingShallowHJOrdering = "Query32bOptJoinTreeOptOrderingShallowHJOrdering"
    Query33aOptJoinTreeOptOrderingShallowHJOrdering = "Query33aOptJoinTreeOptOrderingShallowHJOrdering"
    Query33bOptJoinTreeOptOrderingShallowHJOrdering = "Query33bOptJoinTreeOptOrderingShallowHJOrdering"
    Query33cOptJoinTreeOptOrderingShallowHJOrdering = "Query33cOptJoinTreeOptOrderingShallowHJOrdering"
    Query3aOptJoinTreeOptOrderingShallowHJOrdering = "Query3aOptJoinTreeOptOrderingShallowHJOrdering"
    Query3bOptJoinTreeOptOrderingShallowHJOrdering = "Query3bOptJoinTreeOptOrderingShallowHJOrdering"
    Query3cOptJoinTreeOptOrderingShallowHJOrdering = "Query3cOptJoinTreeOptOrderingShallowHJOrdering"
    Query4aOptJoinTreeOptOrderingShallowHJOrdering = "Query4aOptJoinTreeOptOrderingShallowHJOrdering"
    Query4bOptJoinTreeOptOrderingShallowHJOrdering = "Query4bOptJoinTreeOptOrderingShallowHJOrdering"
    Query4cOptJoinTreeOptOrderingShallowHJOrdering = "Query4cOptJoinTreeOptOrderingShallowHJOrdering"
    Query5aOptJoinTreeOptOrderingShallowHJOrdering = "Query5aOptJoinTreeOptOrderingShallowHJOrdering"
    Query5bOptJoinTreeOptOrderingShallowHJOrdering = "Query5bOptJoinTreeOptOrderingShallowHJOrdering"
    Query5cOptJoinTreeOptOrderingShallowHJOrdering = "Query5cOptJoinTreeOptOrderingShallowHJOrdering"
    Query6aOptJoinTreeOptOrderingShallowHJOrdering = "Query6aOptJoinTreeOptOrderingShallowHJOrdering"
    Query6bOptJoinTreeOptOrderingShallowHJOrdering = "Query6bOptJoinTreeOptOrderingShallowHJOrdering"
    Query6cOptJoinTreeOptOrderingShallowHJOrdering = "Query6cOptJoinTreeOptOrderingShallowHJOrdering"
    Query6dOptJoinTreeOptOrderingShallowHJOrdering = "Query6dOptJoinTreeOptOrderingShallowHJOrdering"
    Query6eOptJoinTreeOptOrderingShallowHJOrdering = "Query6eOptJoinTreeOptOrderingShallowHJOrdering"
    Query6fOptJoinTreeOptOrderingShallowHJOrdering = "Query6fOptJoinTreeOptOrderingShallowHJOrdering"
    Query7aOptJoinTreeOptOrderingShallowHJOrdering = "Query7aOptJoinTreeOptOrderingShallowHJOrdering"
    Query7bOptJoinTreeOptOrderingShallowHJOrdering = "Query7bOptJoinTreeOptOrderingShallowHJOrdering"
    Query7cOptJoinTreeOptOrderingShallowHJOrdering = "Query7cOptJoinTreeOptOrderingShallowHJOrdering"
    Query8aOptJoinTreeOptOrderingShallowHJOrdering = "Query8aOptJoinTreeOptOrderingShallowHJOrdering"
    Query8bOptJoinTreeOptOrderingShallowHJOrdering = "Query8bOptJoinTreeOptOrderingShallowHJOrdering"
    Query8cOptJoinTreeOptOrderingShallowHJOrdering = "Query8cOptJoinTreeOptOrderingShallowHJOrdering"
    Query8dOptJoinTreeOptOrderingShallowHJOrdering = "Query8dOptJoinTreeOptOrderingShallowHJOrdering"
    Query9aOptJoinTreeOptOrderingShallowHJOrdering = "Query9aOptJoinTreeOptOrderingShallowHJOrdering"
    Query9bOptJoinTreeOptOrderingShallowHJOrdering = "Query9bOptJoinTreeOptOrderingShallowHJOrdering"
    Query9cOptJoinTreeOptOrderingShallowHJOrdering = "Query9cOptJoinTreeOptOrderingShallowHJOrdering"
    Query9dOptJoinTreeOptOrderingShallowHJOrdering = "Query9dOptJoinTreeOptOrderingShallowHJOrdering"


def list_all_public_fields(obj):
    ret = []
    for a in dir(obj):
        if not a.startswith('_') and not a.isupper():
            ret.append(a)
    return ret

def get_project_dir():
    return Path.home() / "projects" / "treetracker2-remote-dev"

def get_mvn_root_dir():
    return get_project_dir() / "treeTracker"

def get_template_dir():
    return get_mvn_root_dir() / "treetracker-benchmark" / "src" / "main" / "resources" / "codegen" / "profiling"


def get_benchmark_dir(benchmark: Benchmark):
    return get_mvn_root_dir() / "treetracker-benchmark" / "src" / "main" / "java" / "org" / "zhu45" / "treetracker" / "benchmark" / benchmark


def get_template_path(template_name):
    return get_template_dir() / template_name


def get_algorithm_name(algorithm):
    return algorithm


def get_reduction_time_profiling_class(benchmark: Benchmark, algorithm: Algorithm):
    return f"{algorithm}{benchmark}ReductionTimeProfiling"


def extract_number_from_str(s):
    return re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s)


def generate_java_job(benchmark: Benchmark, algorithm: Algorithm, query: TTJHPJOB):
    environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                     keep_trailing_newline=True)
    template = environment.get_template("ReductionTimeProfilingJOB.javat")
    content = template.render(benchmark=benchmark, algorithmName=get_algorithm_name(algorithm), algorithm=algorithm, query=query, queryNumber=extract_number_from_str(query)[0], runtimes=RUNTIMES)
    filename = get_benchmark_dir(benchmark) / f"{get_reduction_time_profiling_class(benchmark, get_algorithm_name(algorithm))}.java"
    with open(filename, mode="w", encoding="utf-8") as profilingClass:
        profilingClass.write(content)
        print(f"... wrote {filename}")

if __name__ == "__main__":
    generate_java_job(Benchmark.job, Algorithm.TTJ, TTJHPJOB.Query6aOptJoinTreeOptOrderingShallowHJOrdering)
