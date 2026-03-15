"""
Scatter plot of all three benchmarks on native Postgres plans

Fig. 8 becomes 3 plots: HJ v.s. Postgres, TTJ v.s. HJ, TTJ v.s. TTJ^L
"""
import logging
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt
from scipy.stats import gmean

from icde2025.postgres_perf import process_job_raw_data, process_ssb_raw_data, process_tpch_raw_data
from plot.constants import DATA_SOURCE_CSV, COLUMN_RIGHT_BOUND, JOB_SQLITE_ORDERING_RESULTS_INTROW_ON, \
    JOB_POSTGRES_ORDERGING_RESULTS_INTROW_OFF, POSTGRES, HJ, TTJ_LINEAR, TTJ, SSB_SQLITE_ORDERING_RESULTS_INTROW_ON, \
    SSB_POSTGRES_ORDERING_RESULTS_INTROW_OFF, JOB_COLOR, SSB_COLOR, \
    TPCH_SQLITE_ORDERING_RESULTS_INTROW_ON, TPCH_POSTGRES_ORDERING_RESULTS_INTROW_OFF, TPC_COLOR
from plot.cost_model4 import extract_data_from_csv
from plot.job import extract_data_from_csv as extract_data_from_csv2
from plot.utility import check_argument

logging.getLogger('matplotlib.font_manager').disabled = True


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "perf_on_postgres_plans" / csv_name


def get_job_full_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


def get_ssb_full_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / "hj_ordering_hj" / csv_name


def get_ssb_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / "perf_on_postgres_plans" / csv_name


def get_tpch_full_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "tpch" / "with_predicates" / "hj_ordering_hj" / csv_name


def get_tpch_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "tpch" / "with_predicates" / "perf_on_postgres_plans" / csv_name


class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm


# def speedup_analysis(data_speedup, labels):
#     """
#     Compute max, min, geometric mean of speedups for each algorithm
#     """
#     for algorithm in data_speedup:
#         speed_vals = np.array(data_speedup[algorithm])
#         min_idx = np.argmin(speed_vals)
#         min_val = np.min(speed_vals)
#         max_idx = np.argmax(speed_vals)
#         max_val = np.max(speed_vals)
#         geo_mean = gmean(speed_vals)
#         print(f"""
#         algorithm : {algorithm}
#         min       : {min_val}
#         min_query : {labels[min_idx]}
#         max       : {max_val}
#         max_query : {labels[max_idx]}
#         geo_mean  : {geo_mean}
#         """)
# 
# 
# def print_speedup(data_speedup, labels):
#     for algorithm in data_speedup:
#         print(f"{algorithm}")
#         speed_vals = np.array(data_speedup[algorithm])
#         label_speedup_list = []
#         for i in range(len(labels)):
#             label_speedup_list.append(f"{labels[i]}: {speed_vals[i]}")
#         print('\n'.join(label_speedup_list))
# 
# 
# def ttj_perf_analysis(grouped_data, labels):
#     count_ttj_fastest = 0
#     ttj_fastest = np.array([])
#     next_best = np.array([])
#     count_ttj_slower_hj = 0
#     ttj_slower_hj = np.array([])
#     ttj_slower_hj_hj = np.array([])
#     ttj_slower_hj_labels = []
#     total_queries = len(grouped_data[TTJ])
#     ttj_next_best = np.array([])
#     best_when_ttj_is_second = np.array([])
#     ttj_slowest = np.array([])
#     best = np.array([])
# 
#     for idx in range(len(grouped_data[TTJ])):
#         if grouped_data[TTJ][idx] <= grouped_data[HJ][idx]:
#             count_ttj_fastest += 1
#             ttj_fastest = np.append(ttj_fastest, grouped_data[TTJ][idx])
#         else:
#             ttj_next_best = np.append(ttj_next_best, grouped_data[TTJ][idx])
#             best_when_ttj_is_second = np.append(best_when_ttj_is_second, grouped_data[HJ][idx])
#             count_ttj_slower_hj += 1
#             ttj_slower_hj = np.append(ttj_slower_hj, grouped_data[TTJ][idx])
#             ttj_slower_hj_hj = np.append(ttj_slower_hj_hj, grouped_data[HJ][idx])
#             ttj_slower_hj_labels.append(labels[idx])
# 
#     print(f"total queries: {total_queries}, number of queries that TTJ is the fastest: {count_ttj_fastest}")
# 
#     print("analyze TTJ slower than HJ queries ...")
#     ttj_slower_hj_speedup = dict()
#     ttj_slower_hj_speedup[TTJ] = \
#         [round(baseline_time / algorithm_time, 1) for baseline_time, algorithm_time in
#          zip(ttj_slower_hj_hj, ttj_slower_hj)]
#     print(f"ttj_slower_hj_speedup: {ttj_slower_hj_speedup}")
#     if len(ttj_slower_hj_speedup[TTJ]) > 0:
#         speedup_analysis(ttj_slower_hj_speedup, labels)
# 
#     ttj_slower_hj_percentage = [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
#                                 zip(ttj_slower_hj_hj, ttj_slower_hj)]
#     print(f"ttj_slower_hj_percentage: {ttj_slower_hj_percentage}")
#     if len(ttj_slower_hj_percentage) > 0:
#         print(f"the min of ttj_slower_hj_percentage: {min(ttj_slower_hj_percentage)}")
#         ttj_slower_hj_percentage_positive = [-1 * val for val in ttj_slower_hj_percentage]
#         print(f"geometric mean of ttj_slower_hj_percentage: {gmean(ttj_slower_hj_percentage_positive)}")
# 
#     print("analyze the margin of TTJ being the fast algorithm ...")
#     ttj_fastest_speedup = \
#         [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
#          zip(next_best, ttj_fastest)]
#     ttj_fastest_speedup.sort(reverse=True)
#     print(f"ttj_fastest_speedup ({len(ttj_fastest_speedup)}): {ttj_fastest_speedup}")
#     speedup_below_one_percent_count = 0
#     speedup_below_ten_percent_count = 0
#     for speedup in ttj_fastest_speedup:
#         if speedup < 0.01:
#             speedup_below_one_percent_count += 1
#         if speedup < 0.1:
#             speedup_below_ten_percent_count += 1
#     print(f"speedup_below_one_percent_count: {speedup_below_one_percent_count}")
#     print(f"speedup_below_ten_percent_count: {speedup_below_ten_percent_count}")
# 
#     print("analyze the margin of TTJ being the next best algorithm ...")
#     speedup_below_one_percent_next_best_count = 0
#     speedup_below_ten_percent_next_best_count = 0
#     ttj_next_best_speedup = \
#         [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
#          zip(best_when_ttj_is_second, ttj_next_best)]
#     ttj_next_best_speedup.sort(reverse=True)
#     print(f"ttj_next_best_speedup ({len(ttj_next_best_speedup)}): {ttj_next_best_speedup}")
#     for speedup in ttj_next_best_speedup:
#         if speedup > -0.01:
#             speedup_below_one_percent_next_best_count += 1
#         if speedup > -0.1:
#             speedup_below_ten_percent_next_best_count += 1
#     print(f"speedup_below_one_percent_next_best_count: {speedup_below_one_percent_next_best_count}")
#     print(f"speedup_below_ten_percent_next_best_count: {speedup_below_ten_percent_next_best_count}")
# 
#     print("analyze the margin of TTJ being the slowest algorithm ...")
#     speedup_below_one_percent_slowest_count = 0
#     speedup_below_ten_percent_slowest_count = 0
#     ttj_slowest_speedup = \
#         [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
#          zip(best, ttj_slowest)]
#     ttj_slowest_speedup.sort(reverse=True)
#     print(f"ttj_slowest_speedup ({len(ttj_slowest_speedup)}): {ttj_slowest_speedup}")
#     for speedup in ttj_slowest_speedup:
#         if speedup > -0.01:
#             speedup_below_one_percent_slowest_count += 1
#         if speedup > -0.1:
#             speedup_below_ten_percent_slowest_count += 1
#     print(f"speedup_below_one_percent_slowest_count: {speedup_below_one_percent_slowest_count}")
#     print(f"speedup_below_ten_percent_slowest_count: {speedup_below_ten_percent_slowest_count}")
# 
#     def ttj_speedup_analysis(baseline):
#         print(f"Speedup analysis for TTJ over {baseline}")
#         data_speedup = dict()
#         baseline = grouped_data[baseline]
#         data_speedup[TTJ] = \
#             [round(baseline_time / algorithm_time, 1) for baseline_time, algorithm_time in
#              zip(baseline, grouped_data[TTJ])]
#         speedup_analysis(data_speedup, labels)
# 
#     ttj_speedup_analysis(HJ)


def plot(algorithm_pair):
    """
    Fig. 8 becomes 3 plots: HJ v.s. Postgres, TTJ v.s. HJ, TTJ v.s. TTJ^L
    """
    # 
    # def perform_grouping(raw_data: dict) -> dict:
    #     """
    #     We aggregate all the queries with the same flight, e.g., 1a, 1b, 1c together
    #     """
    #     grouped_data = dict()
    #     for algorithm in raw_data.keys():
    #         grouped_data[algorithm] = []
    #         data = raw_data[algorithm]
    #         grouped_data[algorithm].append(data['query1a'])
    #         grouped_data[algorithm].append(data['query1b'])
    #         grouped_data[algorithm].append(data['query1c'])
    #         grouped_data[algorithm].append(data['query1d'])
    #         grouped_data[algorithm].append(data['query2a'])
    #         grouped_data[algorithm].append(data['query2b'])
    #         grouped_data[algorithm].append(data['query2c'])
    #         grouped_data[algorithm].append(data['query2d'])
    #         grouped_data[algorithm].append(data['query3a'])
    #         grouped_data[algorithm].append(data['query3b'])
    #         grouped_data[algorithm].append(data['query3c'])
    #         grouped_data[algorithm].append(data['query4a'])
    #         grouped_data[algorithm].append(data['query4b'])
    #         grouped_data[algorithm].append(data['query4c'])
    #         grouped_data[algorithm].append(data['query5a'])
    #         grouped_data[algorithm].append(data['query5b'])
    #         grouped_data[algorithm].append(data['query5c'])
    #         grouped_data[algorithm].append(data['query6a'])
    #         grouped_data[algorithm].append(data['query6b'])
    #         grouped_data[algorithm].append(data['query6c'])
    #         grouped_data[algorithm].append(data['query6d'])
    #         grouped_data[algorithm].append(data['query6e'])
    #         grouped_data[algorithm].append(data['query6f'])
    #         grouped_data[algorithm].append(data['query7a'])
    #         grouped_data[algorithm].append(data['query7b'])
    #         grouped_data[algorithm].append(data['query7c'])
    #         grouped_data[algorithm].append(data['query8a'])
    #         grouped_data[algorithm].append(data['query8b'])
    #         grouped_data[algorithm].append(data['query8c'])
    #         grouped_data[algorithm].append(data['query8d'])
    #         grouped_data[algorithm].append(data['query9a'])
    #         grouped_data[algorithm].append(data['query9b'])
    #         grouped_data[algorithm].append(data['query9c'])
    #         grouped_data[algorithm].append(data['query9d'])
    #         grouped_data[algorithm].append(data['query10a'])
    #         grouped_data[algorithm].append(data['query10b'])
    #         grouped_data[algorithm].append(data['query10c'])
    #         grouped_data[algorithm].append(data['query11a'])
    #         grouped_data[algorithm].append(data['query11b'])
    #         grouped_data[algorithm].append(data['query11c'])
    #         grouped_data[algorithm].append(data['query11d'])
    #         grouped_data[algorithm].append(data['query12a'])
    #         grouped_data[algorithm].append(data['query12b'])
    #         grouped_data[algorithm].append(data['query12c'])
    #         grouped_data[algorithm].append(data['query13a'])
    #         grouped_data[algorithm].append(data['query13b'])
    #         grouped_data[algorithm].append(data['query13c'])
    #         grouped_data[algorithm].append(data['query13d'])
    #         grouped_data[algorithm].append(data['query14a'])
    #         grouped_data[algorithm].append(data['query14b'])
    #         grouped_data[algorithm].append(data['query14c'])
    #         grouped_data[algorithm].append(data['query15a'])
    #         grouped_data[algorithm].append(data['query15b'])
    #         grouped_data[algorithm].append(data['query15c'])
    #         grouped_data[algorithm].append(data['query15d'])
    #         grouped_data[algorithm].append(data['query16a'])
    #         grouped_data[algorithm].append(data['query16b'])
    #         grouped_data[algorithm].append(data['query16c'])
    #         grouped_data[algorithm].append(data['query16d'])
    #         grouped_data[algorithm].append(data['query17a'])
    #         grouped_data[algorithm].append(data['query17b'])
    #         grouped_data[algorithm].append(data['query17c'])
    #         grouped_data[algorithm].append(data['query17d'])
    #         grouped_data[algorithm].append(data['query17e'])
    #         grouped_data[algorithm].append(data['query17f'])
    #         grouped_data[algorithm].append(data['query18a'])
    #         grouped_data[algorithm].append(data['query18b'])
    #         grouped_data[algorithm].append(data['query18c'])
    #         grouped_data[algorithm].append(data['query19a'])
    #         grouped_data[algorithm].append(data['query19b'])
    #         grouped_data[algorithm].append(data['query19c'])
    #         grouped_data[algorithm].append(data['query19d'])
    #         grouped_data[algorithm].append(data['query20a'])
    #         grouped_data[algorithm].append(data['query20b'])
    #         grouped_data[algorithm].append(data['query20c'])
    #         grouped_data[algorithm].append(data['query21a'])
    #         grouped_data[algorithm].append(data['query21b'])
    #         grouped_data[algorithm].append(data['query21c'])
    #         grouped_data[algorithm].append(data['query22a'])
    #         grouped_data[algorithm].append(data['query22b'])
    #         grouped_data[algorithm].append(data['query22c'])
    #         grouped_data[algorithm].append(data['query22d'])
    #         grouped_data[algorithm].append(data['query23a'])
    #         grouped_data[algorithm].append(data['query23b'])
    #         grouped_data[algorithm].append(data['query23c'])
    #         grouped_data[algorithm].append(data['query24a'])
    #         grouped_data[algorithm].append(data['query24b'])
    #         grouped_data[algorithm].append(data['query25a'])
    #         grouped_data[algorithm].append(data['query25b'])
    #         grouped_data[algorithm].append(data['query25c'])
    #         grouped_data[algorithm].append(data['query26a'])
    #         grouped_data[algorithm].append(data['query26b'])
    #         grouped_data[algorithm].append(data['query26c'])
    #         grouped_data[algorithm].append(data['query27a'])
    #         grouped_data[algorithm].append(data['query27b'])
    #         grouped_data[algorithm].append(data['query27c'])
    #         grouped_data[algorithm].append(data['query28a'])
    #         grouped_data[algorithm].append(data['query28b'])
    #         grouped_data[algorithm].append(data['query28c'])
    #         grouped_data[algorithm].append(data['query29a'])
    #         grouped_data[algorithm].append(data['query29b'])
    #         grouped_data[algorithm].append(data['query29c'])
    #         grouped_data[algorithm].append(data['query30a'])
    #         grouped_data[algorithm].append(data['query30b'])
    #         grouped_data[algorithm].append(data['query30c'])
    #         grouped_data[algorithm].append(data['query31a'])
    #         grouped_data[algorithm].append(data['query31b'])
    #         grouped_data[algorithm].append(data['query31c'])
    #         grouped_data[algorithm].append(data['query32a'])
    #         grouped_data[algorithm].append(data['query32b'])
    #         grouped_data[algorithm].append(data['query33a'])
    #         grouped_data[algorithm].append(data['query33b'])
    #         grouped_data[algorithm].append(data['query33c'])
    #     return grouped_data
    # 
    # def job_analysis(prem_data, prem_data_linear):
    #     data = dict()
    #     for algorithm in prem_data.keys():
    #         if algorithm in [HJ, TTJ, POSTGRES]:
    #             data[algorithm] = prem_data[algorithm]
    #             
    #     data_linear = dict()
    #     for algorithm in prem_data_linear.keys():
    #         if algorithm in [TTJ]:
    #             data_linear[algorithm] = prem_data_linear[algorithm]
    # 
    #     labels = ["1a", "1b", "1c", "1d",
    #               "2a", "2b", "2c", "2d",
    #               "3a", "3b", "3c",
    #               "4a", "4b", "4c",
    #               "5a", "5b", "5c",
    #               "6a", "6b", "6c", "6d", "6e", "6f",
    #               "7a", "7b", "7c",
    #               "8a", "8b", "8c", "8d",
    #               "9a", "9b", "9c", "9d",
    #               "10a", "10b", "10c",
    #               "11a", "11b", "11c", "11d",
    #               "12a", "12b", "12c",
    #               "13a", "13b", "13c", "13d",
    #               "14a", "14b", "14c",
    #               "15a", "15b", "15c", "15d",
    #               "16a", "16b", "16c", "16d",
    #               "17a", "17b", "17c", "17d", "17e", "17f",
    #               "18a", "18b", "18c",
    #               "19a", "19b", "19c", "19d",
    #               "20a", "20b", "20c",
    #               "21a", "21b", "21c",
    #               "22a", "22b", "22c", "22d",
    #               "23a", "23b", "23c",
    #               "24a", "24b",
    #               "25a", "25b", "25c",
    #               "26a", "26b", "26c",
    #               "27a", "27b", "27c",
    #               "28a", "28b", "28c",
    #               "29a", "29b", "29c",
    #               "30a", "30b", "30c",
    #               "31a", "31b", "31c",
    #               "32a", "32b",
    #               "33a", "33b", "33c"]
    #     grouped_data = perform_grouping(data)
    #     grouped_data_linear = perform_grouping(data_linear)
    # 
    #     data_speedup = dict()
    #     for algorithm, dps in grouped_data.items():
    #         ttj_linear = grouped_data_linear[TTJ]
    #         data_speedup[algorithm] = \
    #             [round(ttj_linear_time / algorithm_time, 1) for ttj_linear_time, algorithm_time in
    #              zip(ttj_linear, grouped_data[algorithm])]
    #     del data_speedup[POSTGRES]
    # 
    #     print(f"speedup analysis against {TTJ_LINEAR}")
    #     speedup_analysis(data_speedup, labels)
    #     ttj_perf_analysis(grouped_data, labels)

    job_plot_linear_plan = {
        DATA_SOURCE_CSV: JOB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        COLUMN_RIGHT_BOUND: 114
    }

    job_plot = {
        DATA_SOURCE_CSV: JOB_POSTGRES_ORDERGING_RESULTS_INTROW_OFF,
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])
    prem_data[POSTGRES] = process_job_raw_data()
    prem_data_linear = extract_data_from_csv(get_job_full_path_linear(job_plot_linear_plan[DATA_SOURCE_CSV]),
                                             column_range=[1, job_plot_linear_plan[COLUMN_RIGHT_BOUND]])
    # job_analysis(prem_data, prem_data_linear)

    ssb_plot_linear_plan = {
        # SF=1, IntRow Optimization enabled
        DATA_SOURCE_CSV: SSB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        # SF=1, IntRow Optimization disabled
        # DATA_SOURCE_CSV: "benchmarkssb-result-2024-07-15t23:56:26.1061benchmarkssb-result-2024-12-09t17:27:00.748485benchmarkssb-result-2024-12-11t01:07:14.181061_perf_report.csv",
        COLUMN_RIGHT_BOUND: 14,
    }

    ssb_plot = {
        DATA_SOURCE_CSV: SSB_POSTGRES_ORDERING_RESULTS_INTROW_OFF,
        COLUMN_RIGHT_BOUND: 14,
    }

    ssb_prem_data, _ = extract_data_from_csv2(get_ssb_full_path(ssb_plot[DATA_SOURCE_CSV]),
                                              column_range=[1, ssb_plot[COLUMN_RIGHT_BOUND]])
    ssb_prem_data[POSTGRES] = process_ssb_raw_data()
    ssb_prem_data_linear, _ = extract_data_from_csv2(
        get_ssb_full_path_linear(ssb_plot_linear_plan[DATA_SOURCE_CSV]),
        column_range=[1, ssb_plot_linear_plan[COLUMN_RIGHT_BOUND]])

    tpch_plot_linear_plan = {
        DATA_SOURCE_CSV: TPCH_SQLITE_ORDERING_RESULTS_INTROW_ON,
        COLUMN_RIGHT_BOUND: 14,
    }
    tpch_data_linear, _ = extract_data_from_csv2(get_tpch_full_path_linear(tpch_plot_linear_plan[DATA_SOURCE_CSV]),
                                                 column_range=[1, tpch_plot_linear_plan[COLUMN_RIGHT_BOUND]])
    tpch_plot = {
        DATA_SOURCE_CSV: TPCH_POSTGRES_ORDERING_RESULTS_INTROW_OFF,
        COLUMN_RIGHT_BOUND: 14,
    }
    tpch_data, _ = extract_data_from_csv2(get_tpch_full_path(tpch_plot[DATA_SOURCE_CSV]),
                                          column_range=[1, tpch_plot[COLUMN_RIGHT_BOUND]])
    tpch_data[POSTGRES] = process_tpch_raw_data()

    # Add JOB data
    query_labels = list(prem_data[HJ].keys())
    job_x_axis_algorithm_time, job_y_axis_algorithm_time = [], []

    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == algorithm_pair.x_axis_algorithm:
                job_x_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == algorithm_pair.y_axis_algorithm:
                job_y_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
                
    # print(f"{algorithm_pair.y_axis_algorithm} is faster than {algorithm_pair.x_axis_algorithm} on "
    #       f"{np.sum(np.array(job_x_axis_algorithm_time) > np.array(job_y_axis_algorithm_time))} queries")

    job_ttj_linear_algorithm_time = []
    for query_label in query_labels:
        for algorithm in prem_data_linear:
            if algorithm == TTJ:
                job_ttj_linear_algorithm_time.append(prem_data_linear[algorithm][query_label] / 1000)

    ttj_fastest = 0
    if algorithm_pair.y_axis_algorithm == TTJ and algorithm_pair.x_axis_algorithm == HJ:
        for hj_time, ttj_time, ttj_linear_time in zip(job_x_axis_algorithm_time, job_y_axis_algorithm_time, job_ttj_linear_algorithm_time):
            if ttj_time < hj_time and ttj_time < ttj_linear_time:
                ttj_fastest += 1
    print(f"{algorithm_pair.y_axis_algorithm} is the fastest compared to {algorithm_pair.x_axis_algorithm} and {TTJ_LINEAR} is {ttj_fastest}")
    
    if algorithm_pair.x_axis_algorithm == TTJ_LINEAR:
        check_argument(len(job_x_axis_algorithm_time) == 0, 'x_axis_algorithm_time should be empty')
        for query_label in query_labels:
            for algorithm in prem_data_linear:
                if algorithm == TTJ:
                    job_x_axis_algorithm_time.append(prem_data_linear[algorithm][query_label] / 1000)

    # Add SSB data
    if algorithm_pair.x_axis_algorithm != TTJ_LINEAR:
        ssb_x_axis_algorithm_time = [speed / 1000 for speed in ssb_prem_data[algorithm_pair.x_axis_algorithm]]
    else:
        ssb_x_axis_algorithm_time = [speed / 1000 for speed in ssb_prem_data_linear[TTJ]]
    ssb_y_axis_algorithm_time = [speed / 1000 for speed in ssb_prem_data[algorithm_pair.y_axis_algorithm]]

    # Add TPC-H data
    if algorithm_pair.x_axis_algorithm != TTJ_LINEAR:
        tpch_x_axis_algorithm_time = [speed / 1000 for speed in tpch_data[algorithm_pair.x_axis_algorithm]]
    else:
        tpch_x_axis_algorithm_time = [speed / 1000 for speed in tpch_data_linear[TTJ]]
    tpch_y_axis_algorithm_time = [speed / 1000 for speed in tpch_data[algorithm_pair.y_axis_algorithm]]

    # Create a scatter plot
    f, ax = plt.subplots(figsize=(6, 6))
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_aspect('equal', adjustable='box')

    axis_label_font_size = 20

    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        plt.xlim(0.01, 300)
        plt.ylim(0.01, 300)
        plt.xlabel(r"$\mathsf{PostgreSQL}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR,
                    label="TPC-H")
        plt.scatter(ssb_x_axis_algorithm_time, ssb_y_axis_algorithm_time, s=20, marker="x", color=SSB_COLOR,
                    label="SSB")
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.01, 300)
        plt.ylim(0.01, 300)
        plt.xlabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR,
                    label="TPC-H")
        plt.scatter(ssb_x_axis_algorithm_time, ssb_y_axis_algorithm_time, s=20, marker="x", color=SSB_COLOR,
                    label="SSB")
    elif algorithm_pair.x_axis_algorithm == TTJ_LINEAR and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.01, 300)
        plt.ylim(0.01, 300)
        plt.xlabel(r'$\mathsf{TTJ}^{L}$ time (s)', fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR,
                    label="TPC-H")
        plt.scatter(ssb_x_axis_algorithm_time, ssb_y_axis_algorithm_time, s=20, marker="x", color=SSB_COLOR,
                    label="SSB")

    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 12}
    plt.legend(loc='upper left',
               bbox_to_anchor=(0.01, 1.08),  # Shift legend outside (right of the plot)
               # borderaxespad=0.1,  # Padding between plot and legend
               labelspacing=0.1,
               handletextpad=0.1,
               frameon=False,
               ncol=3,
               prop=font2)

    plt.tight_layout()
    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        plt.savefig("mega-bushy-hj-postgres.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("mega-bushy-ttj-hj.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == TTJ_LINEAR and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("mega-bushy-ttj-ttj-l.pdf", format='pdf')


if __name__ == "__main__":
    plot(AlgorithmPair(HJ, POSTGRES))
    plot(AlgorithmPair(TTJ, HJ))
    plot(AlgorithmPair(TTJ, TTJ_LINEAR))
