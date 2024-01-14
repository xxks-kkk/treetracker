"""
Generate java file based on `ReductionTimeProfiling.javat` for profiling specific algorithm on specific benchmark
"""
from pathlib import Path

import jinja2
from jinja2 import FileSystemLoader


class Benchmark:
    exp2p5 = "exp2p5"


class Algorithm:
    TTJ = "TTJHP"

class Exp2P5Query:
    Exp2P5Query0P = "Exp2P5Query0P"
    Exp2P5Query10P = "Exp2P5Query10P"
    Exp2P5Query20P = "Exp2P5Query20P"
    Exp2P5Query30P = "Exp2P5Query30P"
    Exp2P5Query40P = "Exp2P5Query40P"
    Exp2P5Query50P = "Exp2P5Query50P"
    Exp2P5Query60P = "Exp2P5Query60P"
    Exp2P5Query70P = "Exp2P5Query70P"
    Exp2P5Query80P = "Exp2P5Query80P"
    Exp2P5Query90P = "Exp2P5Query90P"
    Exp2P5Query100P = "Exp2P5Query100P"

def list_all_public_fields(obj):
    ret = []
    for a in dir(obj):
        if not a.startswith('_') and not a.isupper():
            ret.append(a)
    return ret


def get_mvn_root_dir():
    return Path.home() / "projects" / "treetracker2" / "treeTracker"

def get_template_dir():
    return get_mvn_root_dir() / "treetracker-benchmark" / "src" / "main" / "resources" / "codegen" / "profiling"


def get_benchmark_dir(benchmark: Benchmark):
    return get_mvn_root_dir() / "treetracker-benchmark" / "src" / "main" / "java" / "org" / "zhu45" / "treetracker" / "benchmark" / "micro" / benchmark


def get_template_path(template_name):
    return get_template_dir() / template_name

def get_exp2p5_class():
    return "Exp2P5Profiling"


def generate_java(benchmark: Benchmark, algorithm: Algorithm, query: Exp2P5Query):
    environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                     keep_trailing_newline=True)
    template = environment.get_template("Exp2P5Profiling.javat")
    content = template.render(benchmark=benchmark, algorithm=algorithm, query=query)
    filename = get_benchmark_dir(benchmark) / (get_exp2p5_class() + ".java")
    with open(filename, mode="w", encoding="utf-8") as profilingClass:
        profilingClass.write(content)
        print(f"... wrote {filename}")

if __name__ == "__main__":
    generate_java(Benchmark.exp2p5, Algorithm.TTJ, Exp2P5Query.Exp2P5Query0P)
