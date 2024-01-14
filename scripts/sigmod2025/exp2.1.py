"""
Exp2.1: Space consumption of ng
"""
import csv
from pathlib import Path

import numpy as np
from scipy.stats import gmean

from plot.constants import DATA_SOURCE_CSV, TTJ
from plot.utility import to_float


def get_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "simple-cost-model-with-predicates" / csv_name


rkRelationSize = "rkRelationSize"
noGoodListSize = "noGoodListSize"
noGoodListSizeInBytes = "noGoodListSizeInBytes"
evaluationMemoryCostInBytes = "evaluationMemoryCostInBytes"

def argmedian(x):
  return np.argpartition(x, len(x) // 2)[len(x) // 2]


def compute_stats(queries, data, rk_relation_size=None):
    result = dict()
    np_array = np.array(data)
    if rk_relation_size is None:
        result['max'] = [np.max(np_array), queries[np.argmax(np_array)]]
    else:
        result['max'] = [np.max(np_array), np.max(np_array) / rk_relation_size[np.argmax(np_array)], queries[np.argmax(np_array)]]
    if rk_relation_size is None:
        result['min'] = [np.min(np_array), queries[np.argmin(np_array)]]
    else:
        result['min'] = [np.min(np_array), np.min(np_array) / rk_relation_size[np.argmin(np_array)], queries[np.argmin(np_array)]]
    result['avg'] = [np.average(np_array)]
    result['gmean'] = [gmean(np_array)]
    result['median'] = [np.median(np_array), queries[argmedian(np_array)]]
    return result


class Stats:
    def __init__(self, benchmark, queries, logical_data, logical_size_data, rk_relation_size, physical_data):
        self.benchmark = benchmark
        self.queries = queries
        self.rk_relation_size = rk_relation_size
        self.logical_data = logical_data
        self.logical_size_data = logical_size_data
        self.physical_data = physical_data
        self.logical_stats = compute_stats(queries, logical_data)
        self.logical_size_stats = compute_stats(queries, logical_size_data,rk_relation_size)
        self.physical_stats = compute_stats(queries, physical_data)

    def __repr__(self):
        return f""""
        Benchmark: {self.benchmark}
        > no_good_list_size_ratio_logical:
        min:   {self.logical_stats['min'][0]:.1%},        ({self.logical_stats['min'][1]})
        max:   {self.logical_stats['max'][0]:.1%},        ({self.logical_stats['max'][1]})
        avg:   {self.logical_stats['avg'][0]:.1%},
        gmean: {self.logical_stats['gmean'][0]:.1%},   
        med: {self.logical_stats['median'][0]:.1%}        ({self.logical_stats['median'][1]})
        > no_good_list_size:
        min:   {self.logical_size_stats['min'][0]},       ({self.logical_size_stats['min'][1]}) ({self.logical_size_stats['min'][2]})
        max:   {self.logical_size_stats['max'][0]},       ({self.logical_size_stats['max'][1]}) ({self.logical_size_stats['max'][2]})
        avg:   {self.logical_size_stats['avg'][0]},
        gmean: {self.logical_size_stats['gmean'][0]},   
        med:   {self.logical_size_stats['median'][0]}     ({self.logical_size_stats['median'][1]})
        > no_good_list_size_ratio_physical:
        min:   {self.physical_stats['min'][0]:.1%},       ({self.physical_stats['min'][1]})
        max:   {self.physical_stats['max'][0]:.1%},       ({self.physical_stats['max'][1]})
        avg:   {self.physical_stats['avg'][0]:.1%},  
        gmean: {self.physical_stats['gmean'][0]:.1%}, 
        med:   {self.physical_stats['median'][0]:.1%}     ({self.physical_stats['median'][1]})
        """


def extract_data_from_agg_csv(DATA_SOURCE_CSV, get_full_path_func):
    full_data_source_csv = dict()
    for algorithm, csvfile in DATA_SOURCE_CSV.items():
        full_data_source_csv[algorithm] = get_full_path_func(csvfile)
    result = dict()

    for algorithm, csv_file_path in full_data_source_csv.items():
        result[algorithm] = dict()
        with open(csv_file_path, "r") as file:
            csv_file = csv.reader(file)
            headers = next(csv_file)
            queries = headers[1:]
            for row in csv_file:
                if rkRelationSize in row:
                    result[algorithm][rkRelationSize] = [to_float(num) for num in row[1:]]
                elif noGoodListSize in row:
                    result[algorithm][noGoodListSize] = [to_float(num) for num in row[1:]]
                elif noGoodListSizeInBytes in row:
                    result[algorithm][noGoodListSizeInBytes] = [to_float(num) for num in row[1:]]
                elif evaluationMemoryCostInBytes in row:
                    result[algorithm][evaluationMemoryCostInBytes] = [to_float(num) for num in row[1:]]
    return queries, result


def compute_statistics(benchmark, csv_name):
    conf_data = dict()
    conf_data[TTJ] = csv_name
    queries, prem_data = extract_data_from_agg_csv(conf_data, get_full_path)
    rk_relation_size = prem_data[TTJ][rkRelationSize]
    no_good_list_size = prem_data[TTJ][noGoodListSize]
    no_good_list_size_in_bytes = prem_data[TTJ][noGoodListSizeInBytes]
    evaluation_memory_cost_in_bytes = prem_data[TTJ][evaluationMemoryCostInBytes]

    no_good_list_size_logic_ratio = []
    no_good_list_size_physical_ratio = []
    max_no_good_list_size = 0
    max_relation_size = 0
    max_no_good_list_query = ''
    max_no_good_list_size_ratio = 0
    max_no_good_list_logic_ratio = 0
    max_no_good_list_logic_ratio_query = ''
    max_no_good_list_logic_ratio_size = 0
    max_no_good_list_logic_ratio_relation_size = 0
    for i in range(len(no_good_list_size)):
        no_good_list_size_logic_ratio.append(no_good_list_size[i] / rk_relation_size[i])
        if no_good_list_size[i] > max_no_good_list_size:
            max_no_good_list_size = no_good_list_size[i]
            max_relation_size = rk_relation_size[i]
            max_no_good_list_query = queries[i]
            max_no_good_list_size_ratio = max_no_good_list_size / max_relation_size
        if no_good_list_size_logic_ratio[-1] > max_no_good_list_logic_ratio:
            max_no_good_list_logic_ratio = no_good_list_size_logic_ratio[-1]
            max_no_good_list_logic_ratio_query = queries[i]
            max_no_good_list_logic_ratio_size = no_good_list_size[i]
            max_no_good_list_logic_ratio_relation_size = rk_relation_size[i]
        no_good_list_size_physical_ratio.append(no_good_list_size_in_bytes[i] / evaluation_memory_cost_in_bytes[i])

    ssb_stats = Stats(benchmark, queries, no_good_list_size_logic_ratio, no_good_list_size, rk_relation_size, no_good_list_size_physical_ratio)
    print(ssb_stats)
    print(f"""
    max_no_good_list_size                      : {max_no_good_list_size}
    max_no_good_list_query                     : {max_no_good_list_query}
    max_relation_size                          : {max_relation_size}
    max_no_good_list_size_ratio                : {max_no_good_list_size_ratio}
    max_no_good_list_logic_ratio               : {max_no_good_list_logic_ratio}
    max_no_good_list_logic_ratio_query         : {max_no_good_list_logic_ratio_query}
    max_no_good_list_logic_ratio_size          : {max_no_good_list_logic_ratio_size}
    max_no_good_list_logic_ratio_relation_size : {max_no_good_list_logic_ratio_relation_size}
    """)


if __name__ == "__main__":
    compute_statistics("SSB", "TTJHP_SSB_aggregagateStatistics.csv")
    compute_statistics("TPC-H", "TTJHP_TPCH_aggregagateStatistics.csv")
    compute_statistics("JOB", "TTJHP_JOB_aggregagateStatistics.csv")
