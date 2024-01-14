"""
Generate java file based on `ReductionTimeProfiling.javat` for profiling specific algorithm on specific benchmark
"""
from pathlib import Path

import jinja2
from jinja2 import FileSystemLoader


class Benchmark:
    ssb = "ssb"
    job = "job"
    tpch = "tpch"


class Algorithm:
    TTJ = "TTJHP"
    LIP = "LIP"
    Yannakakis = "Yannakakis"
    YannakakisB = "YannakakisB"
    HJ = "HASH_JOIN"


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

class HJTPCH:
    Query10WFindOptJoinTree = "Query10WFindOptJoinTree"
    Query11WFindOptJoinTree = "Query11WFindOptJoinTree"
    Query12WFindOptJoinTree = "Query12WFindOptJoinTree"
    Query14WFindOptJoinTree = "Query14WFindOptJoinTree"
    Query15WFindOptJoinTree = "Query15WFindOptJoinTree"
    Query16WFindOptJoinTree = "Query16WFindOptJoinTree"
    Query18WFindOptJoinTree = "Query18WFindOptJoinTree"
    Query19aWFindOptJoinTree = "Query19aWFindOptJoinTree"
    Query19bWFindOptJoinTree = "Query19bWFindOptJoinTree"
    Query19cWFindOptJoinTree = "Query19cWFindOptJoinTree"
    Query20WFindOptJoinTree = "Query20WFindOptJoinTree"
    Query3WFindOptJoinTree = "Query3WFindOptJoinTree"
    Query7aWFindOptJoinTree = "Query7aWFindOptJoinTree"
    Query7bWFindOptJoinTree = "Query7bWFindOptJoinTree"
    Query8WFindOptJoinTree = "Query8WFindOptJoinTree"
    Query9WFindOptJoinTree = "Query9WFindOptJoinTree"


class HJTTJORDERTPCH:
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

class HJYAORDERTPCH:
    Query10WOptJoinTreeOptOrderingHJOnY = "Query10WOptJoinTreeOptOrderingHJOnY"
    Query11WOptJoinTreeOptOrderingHJOnY = "Query11WOptJoinTreeOptOrderingHJOnY"
    Query12WOptJoinTreeOptOrderingHJOnY = "Query12WOptJoinTreeOptOrderingHJOnY"
    Query14WOptJoinTreeOptOrderingHJOnY = "Query14WOptJoinTreeOptOrderingHJOnY"
    Query15WOptJoinTreeOptOrderingHJOnY = "Query15WOptJoinTreeOptOrderingHJOnY"
    Query16WOptJoinTreeOptOrderingHJOnY = "Query16WOptJoinTreeOptOrderingHJOnY"
    Query18WOptJoinTreeOptOrderingHJOnY = "Query18WOptJoinTreeOptOrderingHJOnY"
    Query19aWOptJoinTreeOptOrderingHJOnY = "Query19aWOptJoinTreeOptOrderingHJOnY"
    Query19bWOptJoinTreeOptOrderingHJOnY = "Query19bWOptJoinTreeOptOrderingHJOnY"
    Query19cWOptJoinTreeOptOrderingHJOnY = "Query19cWOptJoinTreeOptOrderingHJOnY"
    Query20WOptJoinTreeOptOrderingHJOnY = "Query20WOptJoinTreeOptOrderingHJOnY"
    Query3WOptJoinTreeOptOrderingHJOnY = "Query3WOptJoinTreeOptOrderingHJOnY"
    Query7aWOptJoinTreeOptOrderingHJOnY = "Query7aWOptJoinTreeOptOrderingHJOnY"
    Query7bWOptJoinTreeOptOrderingHJOnY = "Query7bWOptJoinTreeOptOrderingHJOnY"
    Query8WOptJoinTreeOptOrderingHJOnY = "Query8WOptJoinTreeOptOrderingHJOnY"
    Query9WOptJoinTreeOptOrderingHJOnY = "Query9WOptJoinTreeOptOrderingHJOnY"


def list_all_public_fields(obj):
    ret = []
    for a in dir(obj):
        if not a.startswith('_') and not a.isupper():
            ret.append(a)
    return ret


def get_mvn_root_dir():
    return Path.home() / "projects" / "treetracker2-remote-dev" / "treeTracker"

def get_template_dir():
    return get_mvn_root_dir() / "treetracker-benchmark" / "src" / "main" / "resources" / "codegen" / "collectStats"


def get_benchmark_dir():
    return get_mvn_root_dir() / "treetracker-benchmark" / "src" / "main" / "java" / "org" / "zhu45" / "treetracker" / "benchmark" / "hvt"


def get_template_path(template_name):
    return get_template_dir() / template_name

def get_reduction_time_profiling_class(benchmark: Benchmark, algorithm: Algorithm):
    return f"CollectStats"


def generate_java(algorithm: Algorithm, query):
    environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                     keep_trailing_newline=True)
    template = environment.get_template("CollectTPCHStats.javat")
    content = template.render(algorithm=algorithm, query=query)
    filename = get_benchmark_dir() / f"CollectTPCHStats.java"
    with open(filename, mode="w", encoding="utf-8") as profilingClass:
        profilingClass.write(content)
        print(f"... wrote {filename}")

if __name__ == "__main__":
    generate_java(Algorithm.TTJ, SSBQuery.Query2P1)
