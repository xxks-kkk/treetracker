"""
Generate java file based on `ReductionTimeProfiling.javat` for profiling specific algorithm on specific benchmark
"""
import re
from pathlib import Path

import jinja2
from jinja2 import FileSystemLoader

RUNTIMES=50

class Benchmark:
    ssb = "ssb"
    job = "job"
    tpch = "tpch"


class Algorithm:
    TTJ = "TTJHP"
    LIP = "LIP"
    Yannakakis = "Yannakakis"
    YannakakisB = "YannakakisB"
    Yannakakis1Pass = "Yannakakis1Pass"
    HJ =  "HASH_JOIN"


class SSBQuery:
    Query1P1 = "Query1P1"
    Query1P2 = "Query1P2"
    Query1P3 = "Query1P3"
    Query2P1 = "Query2P1"
    Query2P2 = "Query2P2"
    Query2P3 = "Query2P3"
    Query3P1 = "Query3P1"
    Query3P2 = "Query3P2"
    Query3P3 = "Query3P3"
    Query3P4 = "Query3P4"
    Query4P1 = "Query4P1"
    Query4P2 = "Query4P2"
    Query4P3 = "Query4P3"


class TTJTPCH:
    Query10WOptJoinTreeOptOrdering = "Query10WOptJoinTreeOptOrdering"
    Query11WOptJoinTreeOptOrdering = "Query11WOptJoinTreeOptOrdering"
    Query12WOptJoinTreeOptOrdering = "Query12WOptJoinTreeOptOrdering"
    Query14WOptJoinTreeOptOrdering = "Query14WOptJoinTreeOptOrdering"
    Query15WOptJoinTreeOptOrdering = "Query15WOptJoinTreeOptOrdering"
    Query16WOptJoinTreeOptOrdering = "Query16WOptJoinTreeOptOrdering"
    Query18WOptJoinTreeOptOrdering = "Query18WOptJoinTreeOptOrdering"
    Query19aWOptJoinTreeOptOrdering = "Query19aWOptJoinTreeOptOrdering"
    Query19bWOptJoinTreeOptOrdering = "Query19bWOptJoinTreeOptOrdering"
    Query19cWOptJoinTreeOptOrdering = "Query19cWOptJoinTreeOptOrdering"
    Query20WOptJoinTreeOptOrdering = "Query20WOptJoinTreeOptOrdering"
    Query3WOptJoinTreeOptOrdering = "Query3WOptJoinTreeOptOrdering"
    Query7aWOptJoinTreeOptOrdering = "Query7aWOptJoinTreeOptOrdering"
    Query7bWOptJoinTreeOptOrdering = "Query7bWOptJoinTreeOptOrdering"
    Query8WOptJoinTreeOptOrdering = "Query8WOptJoinTreeOptOrdering"
    Query9WOptJoinTreeOptOrdering = "Query9WOptJoinTreeOptOrdering"

class YannakakisTPCH:
    Query10WOptJoinTreeOptOrderingY = "Query10WOptJoinTreeOptOrderingY"
    Query11WOptJoinTreeOptOrderingY = "Query11WOptJoinTreeOptOrderingY"
    Query12WOptJoinTreeOptOrderingY = "Query12WOptJoinTreeOptOrderingY"
    Query14WOptJoinTreeOptOrderingY = "Query14WOptJoinTreeOptOrderingY"
    Query15WOptJoinTreeOptOrderingY = "Query15WOptJoinTreeOptOrderingY"
    Query16WOptJoinTreeOptOrderingY = "Query16WOptJoinTreeOptOrderingY"
    Query18WOptJoinTreeOptOrderingY = "Query18WOptJoinTreeOptOrderingY"
    Query19aWOptJoinTreeOptOrderingY = "Query19aWOptJoinTreeOptOrderingY"
    Query19bWOptJoinTreeOptOrderingY = "Query19bWOptJoinTreeOptOrderingY"
    Query19cWOptJoinTreeOptOrderingY = "Query19cWOptJoinTreeOptOrderingY"
    Query20WOptJoinTreeOptOrderingY = "Query20WOptJoinTreeOptOrderingY"
    Query3WOptJoinTreeOptOrderingY = "Query3WOptJoinTreeOptOrderingY"
    Query7aWOptJoinTreeOptOrderingY = "Query7aWOptJoinTreeOptOrderingY"
    Query7bWOptJoinTreeOptOrderingY = "Query7bWOptJoinTreeOptOrderingY"
    Query8WOptJoinTreeOptOrderingY = "Query8WOptJoinTreeOptOrderingY"
    Query9WOptJoinTreeOptOrderingY = "Query9WOptJoinTreeOptOrderingY"

class YannakakisBTPCH:
    Query10WOptJoinTreeOptOrderingYB = "Query10WOptJoinTreeOptOrderingYB"
    Query11WOptJoinTreeOptOrderingYB = "Query11WOptJoinTreeOptOrderingYB"
    Query12WOptJoinTreeOptOrderingYB = "Query12WOptJoinTreeOptOrderingYB"
    Query14WOptJoinTreeOptOrderingYB = "Query14WOptJoinTreeOptOrderingYB"
    Query15WOptJoinTreeOptOrderingYB = "Query15WOptJoinTreeOptOrderingYB"
    Query16WOptJoinTreeOptOrderingYB = "Query16WOptJoinTreeOptOrderingYB"
    Query18WOptJoinTreeOptOrderingYB = "Query18WOptJoinTreeOptOrderingYB"
    Query19aWOptJoinTreeOptOrderingYB = "Query19aWOptJoinTreeOptOrderingYB"
    Query19bWOptJoinTreeOptOrderingYB = "Query19bWOptJoinTreeOptOrderingYB"
    Query19cWOptJoinTreeOptOrderingYB = "Query19cWOptJoinTreeOptOrderingYB"
    Query20WOptJoinTreeOptOrderingYB = "Query20WOptJoinTreeOptOrderingYB"
    Query3WOptJoinTreeOptOrderingYB = "Query3WOptJoinTreeOptOrderingYB"
    Query7aWOptJoinTreeOptOrderingYB = "Query7aWOptJoinTreeOptOrderingYB"
    Query7bWOptJoinTreeOptOrderingYB = "Query7bWOptJoinTreeOptOrderingYB"
    Query8WOptJoinTreeOptOrderingYB = "Query8WOptJoinTreeOptOrderingYB"
    Query9WOptJoinTreeOptOrderingYB = "Query9WOptJoinTreeOptOrderingYB"

class TTJHPJOB:
    # Query10aOptJoinTreeOptOrderingShallowHJOrdering = "Query10aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query10bOptJoinTreeOptOrderingShallowHJOrdering = "Query10bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query10cOptJoinTreeOptOrderingShallowHJOrdering = "Query10cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query11aOptJoinTreeOptOrderingShallowHJOrdering = "Query11aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query11bOptJoinTreeOptOrderingShallowHJOrdering = "Query11bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query11cOptJoinTreeOptOrderingShallowHJOrdering = "Query11cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query11dOptJoinTreeOptOrderingShallowHJOrdering = "Query11dOptJoinTreeOptOrderingShallowHJOrdering"
    # Query12aOptJoinTreeOptOrderingShallowHJOrdering = "Query12aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query12bOptJoinTreeOptOrderingShallowHJOrdering = "Query12bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query12cOptJoinTreeOptOrderingShallowHJOrdering = "Query12cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query13aOptJoinTreeOptOrderingShallowHJOrdering = "Query13aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query13bOptJoinTreeOptOrderingShallowHJOrdering = "Query13bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query13cOptJoinTreeOptOrderingShallowHJOrdering = "Query13cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query13dOptJoinTreeOptOrderingShallowHJOrdering = "Query13dOptJoinTreeOptOrderingShallowHJOrdering"
    # Query14aOptJoinTreeOptOrderingShallowHJOrdering = "Query14aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query14bOptJoinTreeOptOrderingShallowHJOrdering = "Query14bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query14cOptJoinTreeOptOrderingShallowHJOrdering = "Query14cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query15aOptJoinTreeOptOrderingShallowHJOrdering = "Query15aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query15bOptJoinTreeOptOrderingShallowHJOrdering = "Query15bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query15cOptJoinTreeOptOrderingShallowHJOrdering = "Query15cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query15dOptJoinTreeOptOrderingShallowHJOrdering = "Query15dOptJoinTreeOptOrderingShallowHJOrdering"
    # Query16aOptJoinTreeOptOrderingShallowHJOrdering = "Query16aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query16bOptJoinTreeOptOrderingShallowHJOrdering = "Query16bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query16cOptJoinTreeOptOrderingShallowHJOrdering = "Query16cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query16dOptJoinTreeOptOrderingShallowHJOrdering = "Query16dOptJoinTreeOptOrderingShallowHJOrdering"
    # Query17aOptJoinTreeOptOrderingShallowHJOrdering = "Query17aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query17bOptJoinTreeOptOrderingShallowHJOrdering = "Query17bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query17cOptJoinTreeOptOrderingShallowHJOrdering = "Query17cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query17dOptJoinTreeOptOrderingShallowHJOrdering = "Query17dOptJoinTreeOptOrderingShallowHJOrdering"
    # Query17eOptJoinTreeOptOrderingShallowHJOrdering = "Query17eOptJoinTreeOptOrderingShallowHJOrdering"
    # Query17fOptJoinTreeOptOrderingShallowHJOrdering = "Query17fOptJoinTreeOptOrderingShallowHJOrdering"
    # Query18aOptJoinTreeOptOrderingShallowHJOrdering = "Query18aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query18bOptJoinTreeOptOrderingShallowHJOrdering = "Query18bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query18cOptJoinTreeOptOrderingShallowHJOrdering = "Query18cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query19aOptJoinTreeOptOrderingShallowHJOrdering = "Query19aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query19bOptJoinTreeOptOrderingShallowHJOrdering = "Query19bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query19cOptJoinTreeOptOrderingShallowHJOrdering = "Query19cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query19dOptJoinTreeOptOrderingShallowHJOrdering = "Query19dOptJoinTreeOptOrderingShallowHJOrdering"
    # Query1aOptJoinTreeOptOrderingShallowHJOrdering = "Query1aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query1bOptJoinTreeOptOrderingShallowHJOrdering = "Query1bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query1cOptJoinTreeOptOrderingShallowHJOrdering = "Query1cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query32aOptJoinTreeOptOrderingShallowHJOrdering = "Query32aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query7bOptJoinTreeOptOrderingShallowHJOrdering = "Query7bOptJoinTreeOptOrderingShallowHJOrdering"
    Query6aOptJoinTreeOptOrderingShallowHJOrdering = "Query6aOptJoinTreeOptOrderingShallowHJOrdering"
    # Query6bOptJoinTreeOptOrderingShallowHJOrdering = "Query6bOptJoinTreeOptOrderingShallowHJOrdering"
    # Query6cOptJoinTreeOptOrderingShallowHJOrdering = "Query6cOptJoinTreeOptOrderingShallowHJOrdering"
    # Query6dOptJoinTreeOptOrderingShallowHJOrdering = "Query6dOptJoinTreeOptOrderingShallowHJOrdering"
    # Query6eOptJoinTreeOptOrderingShallowHJOrdering = "Query6eOptJoinTreeOptOrderingShallowHJOrdering"

class Yannakakis1PassJOB:
    # Query11bOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query11bOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query12bOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query12bOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query17aOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query17aOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query18aOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query18aOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query32aOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query32aOptJoinTreeOptOrderingY1PShallowHJOrdering"
    Query6aOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query6aOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query6bOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query6bOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query6cOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query6cOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query6dOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query6dOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query6eOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query6eOptJoinTreeOptOrderingY1PShallowHJOrdering"
    # Query7bOptJoinTreeOptOrderingY1PShallowHJOrdering = "Query7bOptJoinTreeOptOrderingY1PShallowHJOrdering"

class HASHJOINJOB:
    Query5aFindOptJoinTree = "Query5aFindOptJoinTree"


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
    """
    Work around the checkStyle issue where we cannot use HASH_JOIN as part of Java class name.
    """
    if algorithm == Algorithm.HJ:
        return "HJ"
    else:
        return algorithm


def get_reduction_time_profiling_class(benchmark: Benchmark, algorithm: Algorithm):
    return f"{algorithm}{benchmark}ReductionTimeProfiling"


def extract_number_from_str(s):
    return re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s)


def generate_java_job(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery):
    environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                     keep_trailing_newline=True)
    template = environment.get_template("ReductionTimeProfilingJOB.javat")
    content = template.render(benchmark=benchmark, algorithmName=get_algorithm_name(algorithm), algorithm=algorithm, query=query, queryNumber=extract_number_from_str(query)[0], runtimes=RUNTIMES)
    filename = get_benchmark_dir(benchmark) / f"{get_reduction_time_profiling_class(benchmark, get_algorithm_name(algorithm))}.java"
    with open(filename, mode="w", encoding="utf-8") as profilingClass:
        profilingClass.write(content)
        print(f"... wrote {filename}")


def generate_java(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery):
    if benchmark == Benchmark.job:
        generate_java_job(benchmark, algorithm, query)
    else:
        environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                         keep_trailing_newline=True)
        template = environment.get_template("ReductionTimeProfiling.javat")
        content = template.render(benchmark=benchmark, algorithm=algorithm, query=query, runtimes=RUNTIMES)
        filename = get_benchmark_dir(benchmark) / f"{get_reduction_time_profiling_class(benchmark, algorithm)}.java"
        with open(filename, mode="w", encoding="utf-8") as profilingClass:
            profilingClass.write(content)
            print(f"... wrote {filename}")

if __name__ == "__main__":
    # generate_java(Benchmark.ssb, Algorithm.TTJ, SSBQuery.Query2P1)
    # generate_java(Benchmark.job, Algorithm.TTJ, TTJHPJOB.Query6aOptJoinTreeOptOrderingShallowHJOrdering)
    generate_java(Benchmark.job, Algorithm.HJ, HASHJOINJOB.Query5aFindOptJoinTree)
