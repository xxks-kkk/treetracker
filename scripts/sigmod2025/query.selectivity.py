"""
Query selectivity figure based on Bailu's Figure 8
"""
from pathlib import Path

import numpy as np

from plot import job
from plot.constants import TTJ, HJ
from plot.cost_model4 import extract_data_from_csv

tpch_data = "benchmarktpchwithpredicatesdifferentordering-result-2023-07-06t17:42:23.652904benchmarktpchwithpredicatesdifferentordering-result-2023-07-07t13:48:17.442581benchmarktpchwithpredicatesdifferentordering-result-2023-08-04t16:43:41.041541_perf_report.csv"

def get_tpch_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "tpch" / "with_predicates" / csv_name

ssb_data = "benchmarkssb-result-2023-10-26t12:14:04.768421_perf_report.csv"

def get_ssb_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / csv_name

job_data = "2023-11-27T00:50:36.174069Z_perf_report.csv"

def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / csv_name

def group_queries_based_on_execution_time(expected_dps_len, full_csv_path):
    algorithms = [HJ, TTJ]
    prem_data = extract_data_from_csv(full_csv_path,
                                         column_range=[1, expected_dps_len])
    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in algorithms:
            data[algorithm] = prem_data[algorithm]

    hj_original_labels = list(prem_data[HJ].keys())
    hj_execution_time = []
    for hj_original_label in hj_original_labels:
        hj_execution_time.append(prem_data[HJ][hj_original_label])
    hj_original_idx = np.array(hj_execution_time).argsort()[::-1]

    plot_data = dict()
    hj_expensive_original_idx = hj_original_idx[:int(expected_dps_len/3)]
    for algorithm in algorithms:
        if algorithm not in plot_data:
            plot_data[algorithm] = dict()
        expensive_execution_time = 0
        for i in hj_expensive_original_idx:
            label = hj_original_labels[i]
            expensive_execution_time += prem_data[algorithm][label]
        plot_data[algorithm]['expensive'] = expensive_execution_time

    hj_medium_original_idx = hj_original_idx[int(expected_dps_len/3): int(expected_dps_len/3*2)]
    for algorithm in algorithms:
        if algorithm not in plot_data:
            plot_data[algorithm] = dict()
        medium_execution_time = 0
        for i in hj_medium_original_idx:
            label = hj_original_labels[i]
            medium_execution_time += prem_data[algorithm][label]
        plot_data[algorithm]['medium'] = medium_execution_time

    hj_cheap_original_idx = hj_original_idx[int(expected_dps_len/3*2):]
    for algorithm in algorithms:
        if algorithm not in plot_data:
            plot_data[algorithm] = dict()
        cheap_execution_time = 0
        for i in hj_cheap_original_idx:
            label = hj_original_labels[i]
            cheap_execution_time += prem_data[algorithm][label]
        plot_data[algorithm]['cheap'] = cheap_execution_time
    print(plot_data)

    print(f"expensive improvement %: {(plot_data[HJ]['expensive'] - plot_data[TTJ]['expensive'])/plot_data[HJ]['expensive']}")
    print(f"medium improvement %: {(plot_data[HJ]['medium'] - plot_data[TTJ]['medium'])/plot_data[HJ]['medium']}")
    print(f"cheap improvement %: {(plot_data[HJ]['cheap'] - plot_data[TTJ]['cheap'])/plot_data[HJ]['cheap']}")


if __name__ == "__main__":
    group_queries_based_on_execution_time(113, get_job_full_path(job_data))
    group_queries_based_on_execution_time(14, get_tpch_full_path(tpch_data))
    group_queries_based_on_execution_time(14, get_ssb_full_path(ssb_data))
