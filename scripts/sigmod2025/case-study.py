"""
Case Study
"""
import json
from pathlib import Path

from plot.utility import to_float


def get_plan_statistics_full_path(json_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "simple-cost-model-with-predicates" / json_name

def extract_data_from_json(DATA_SOURCE_JSON, get_full_path_func):
    benchmark_json_full_path = get_full_path_func(DATA_SOURCE_JSON)
    result = dict()
    with open(benchmark_json_full_path, "r") as read_file:
        json_result = json.load(read_file)
        return json_result['Aggregation Stats']

def get_dupratio(json_file):
    aggregation_stats = extract_data_from_json(json_file, get_plan_statistics_full_path)
    set_B_size = to_float(aggregation_stats['noGoodListSize'])
    set_A_size = to_float(aggregation_stats['totalTuplesFiltered'])
    rk_tuples = to_float(aggregation_stats['rkRelationSize'])
    if set_B_size + set_A_size != 0:
        dup_ratio = set_A_size / (set_B_size + set_A_size)
    else:
        dup_ratio = 0
    dangling_ratio = (set_B_size + set_A_size) / rk_tuples
    print(f"""
    json_file :      {json_file}
    rk_tuples :      {rk_tuples}
    dup_ratio :      {dup_ratio}
    dangling_ratio : {dangling_ratio}
    """)


if __name__ == "__main__":
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query7aWOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query7bWOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query8WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query10WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query11WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query12WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query14WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query16WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query18WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query19aWOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query19bWOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query19cWOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query20WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query3WOptJoinTreeOptOrdering.json")
    get_dupratio("TTJHP_org.zhu45.treetracker.benchmark.tpch.Query9WOptJoinTreeOptOrdering.json")
