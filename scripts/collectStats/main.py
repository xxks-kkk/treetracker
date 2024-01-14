"""
Main to drive exp 1.3 Runtime breakdown

The steps are:
1. Generate java file based on `ReductionTimeProfiling.javat` for profiling specific algorithm on specific benchmark
2. Execute `mvn clean install -DskipTests -P prod` to build the source
3. Perform profiling on the engine and generate profile result
4. Parse the profile result and produce statistics related to reduction time percentage
"""
import csv
import subprocess

import jinja2
from jinja2 import FileSystemLoader

from collectStats.proggen import generate_java, get_mvn_root_dir, get_template_dir, Algorithm, list_all_public_fields, \
    TTJTPCH, HJTPCH, YannakakisTPCH, YannakakisBTPCH, HJTTJORDERTPCH, HJYAORDERTPCH
from plot.utility import check_argument


def mvn_bld():
    return_code = subprocess.call("mvn clean install -DskipTests -P stats", shell=True, cwd=get_mvn_root_dir())
    return return_code


def run(algorithm: Algorithm, query):
    generate_java(algorithm, query)
    return_code = mvn_bld()
    check_argument(return_code == 0, f"mvn bld fail: {return_code}")
    create_script()
    return_code = execute_profile_script()
    check_argument(return_code == 0, f"execute script fail: {return_code}")


def execute_profile_script():
    return_code = subprocess.call(f"{get_profile_script_name()}", shell=True, cwd=get_mvn_root_dir())
    return return_code
    

def create_script():
    environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                     keep_trailing_newline=True)
    template = environment.get_template("collectTPCHStats.sht")
    content = template.render()
    filename = get_profile_script_name()
    with open(filename, mode="w", encoding="utf-8") as profile_script:
        profile_script.write(content)
        print(f"... wrote {filename}")
        make_profile_executable(filename)

def make_profile_executable(profile_script_name):
    subprocess.call(f"chmod 755 {profile_script_name}", shell=True, cwd=get_mvn_root_dir())


def get_profile_script_name():
    return get_mvn_root_dir() / f"collectTPCHStats.sh"


def profile_tpch():
    hj_tpch_queries = list_all_public_fields(HJTPCH)
    for query in hj_tpch_queries:
        run(Algorithm.HJ, query)
    ttjhp_tpch_queries = list_all_public_fields(TTJTPCH)
    for query in ttjhp_tpch_queries:
        run(Algorithm.TTJ, query)
    yannakakis_tpch_queries = list_all_public_fields(YannakakisTPCH)
    for query in yannakakis_tpch_queries:
        run(Algorithm.Yannakakis, query)
    yannakakisB_tpch_queries = list_all_public_fields(YannakakisBTPCH)
    for query in yannakakisB_tpch_queries:
        run(Algorithm.YannakakisB, query)
    hj_ttj_order_tpch_queries = list_all_public_fields(HJTTJORDERTPCH)
    for query in hj_ttj_order_tpch_queries:
        run(Algorithm.HJ, query)
    hj_ya_order_tpch_queries = list_all_public_fields(HJYAORDERTPCH)
    for query in hj_ya_order_tpch_queries:
        run(Algorithm.HJ, query)


if __name__ == "__main__":
    profile_tpch()
