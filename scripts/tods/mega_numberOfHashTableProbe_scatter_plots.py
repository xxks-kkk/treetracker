"""
Scatter plot for all three benchmarks with HJ count on the x-axis and TTJ count on the y-axis
 
TTJ vs. YA on all three benchmarks (replacing TTJ vs. YA in Figure 5 and 6)
TTJ vs. HJ in terms of number of hash probes in linear plan
TTJ vs. YA in terms of number of hash probes in linear plan

Yes linear plans first, but also great if you can get for bushy plans
"""
import csv
import logging
import re
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt

from plot.constants import DATA_SOURCE_CSV, TTJ, HJ, Yannakakis1Pass, SSB_COLOR, TPC_COLOR, JOB_COLOR
from plot.utility import to_float

logging.getLogger('matplotlib.font_manager').disabled = True

SSB_HJ_AGG_STATS = "HASH_JOIN_SSB_aggregagateStatistics.csv"
SSB_TTJ_AGG_STATS = "TTJHP_SSB_aggregagateStatistics.csv"
SSB_Yannakakis1Pass_AGG_STATS = "Yannakakis1Pass_SSB_aggregagateStatistics.csv"
TPCH_HJ_AGG_STATS = "HASH_JOIN_TPCH_aggregagateStatistics.csv"
TPCH_TTJ_AGG_STATS = "TTJHP_TPCH_aggregagateStatistics.csv"
TPCH_Yannakakis1Pass_AGG_STATS = "Yannakakis1Pass_TPCH_aggregagateStatistics.csv"
JOB_HJ_AGG_STATS = "HASH_JOIN_JOB_aggregagateStatistics.csv"
JOB_TTJ_AGG_STATS = "TTJHP_JOB_aggregagateStatistics.csv"
JOB_Yannakakis1Pass_AGG_STATS = "Yannakakis1Pass_JOB_aggregagateStatistics.csv"

totalIntermediateResultsProducedWithoutNULL = "totalIntermediateResultsProducedWithoutNULL"
number_of_hash_table_probe_within_full_reducer = "numberOfHashTableProbeWithinFullReducer"


# def get_job_full_path(csv_name):
#     return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "perf_on_postgres_plans" / csv_name
# 
# 
def get_job_stats_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "simple-cost-model-with-predicates" / "hj_ordering_hj" / "job" / csv_name


def get_ssb_stats_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "simple-cost-model-with-predicates" / "hj_ordering_hj" / "ssb" / csv_name


# def get_ssb_stats_path(csv_name):
#     return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / "perf_on_postgres_plans" / csv_name
# 
# 
def get_tpch_stats_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "simple-cost-model-with-predicates" / "hj_ordering_hj" / "tpch" / csv_name


# 
# def get_tpch_full_path(csv_name):
#     return Path.home() / "projects" / "treetracker2" / "results" / "tpch" / "with_predicates" / "perf_on_postgres_plans" / csv_name

class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm


def extract_number_from_str(s):
    return re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s)


def cleanup_csv(DATA_SOURCE_CSV, get_full_path_func):
    """
    For TPC-H, we combine
    - 7a with 7b
    - 19a, 19b, 19c together
    """
    full_data_source_csv = dict()
    for algorithm, csvfile in DATA_SOURCE_CSV.items():
        full_data_source_csv[algorithm] = get_full_path_func(csvfile)
    result = dict()
    for algorithm, csv_file_path in full_data_source_csv.items():
        result[algorithm] = dict()
        with open(csv_file_path) as file:
            csv_file = csv.reader(file)
            headers = next(csv_file)
            headers_tmp = []
            rows_tmp = []
            for header in headers:
                if header == 'statistics':
                    headers_tmp.append(header)
                else:
                    headers_tmp.append(int(extract_number_from_str(header)[0]))
            for row in csv_file:
                rows_tmp.append(row)
            seven_idx = []
            nineteen_idx = []
            for i in range(len(headers_tmp)):
                if headers_tmp[i] == 7:
                    seven_idx.append(i)
                elif headers_tmp[i] == 19:
                    nineteen_idx.append(i)
            rows_use = []
            for row in rows_tmp:
                rows_use.append([])
                for i in range(len(headers_tmp)):
                    if i == seven_idx[0]:
                        val = 0
                        for idx in seven_idx:
                            val += int(row[idx])
                        rows_use[-1].append(val)
                    elif i == nineteen_idx[0]:
                        val = 0
                        for idx in nineteen_idx:
                            val += int(row[idx])
                        rows_use[-1].append(val)
                    elif i in seven_idx or i in nineteen_idx:
                        pass
                    elif i == 0:
                        rows_use[-1].append(row[i])
                    else:
                        rows_use[-1].append(int(row[i]))
            headers_use = []
            for i in range(len(headers_tmp)):
                if i == seven_idx[0]:
                    headers_use.append(7)
                elif i == nineteen_idx[0]:
                    headers_use.append(19)
                elif i in seven_idx or i in nineteen_idx:
                    pass
                else:
                    headers_use.append(headers_tmp[i])
            headers_no_fileds = headers_use[1:]
            idx = np.argsort(headers_no_fileds)
            headers_sorted = [headers_use[0]]
            for i in idx:
                headers_sorted.append(headers_no_fileds[i])
            rows_sorted = []
            for row in rows_use:
                rows_sorted.append([row[0]])
                row_no_fields = row[1:]
                for i in idx:
                    rows_sorted[-1].append(row_no_fields[i])
            for row in rows_sorted:
                if totalIntermediateResultsProducedWithoutNULL in row:
                    result[algorithm][totalIntermediateResultsProducedWithoutNULL] = [to_float(num) for num in row[1:]]
                elif number_of_hash_table_probe_within_full_reducer in row:
                    result[algorithm][totalIntermediateResultsProducedWithoutNULL] = [x + y for x, y in
                                                                                      zip(result[algorithm][
                                                                                              totalIntermediateResultsProducedWithoutNULL],
                                                                                          [to_float(num) for num in
                                                                                           row[1:]])]
    return result


def extract_data_from_agg_csv(DATA_SOURCE_CSV, get_full_path_func):
    full_data_source_csv = dict()
    for algorithm, csvfile in DATA_SOURCE_CSV.items():
        full_data_source_csv[algorithm] = get_full_path_func(csvfile)
    result = dict()

    for algorithm, csv_file_path in full_data_source_csv.items():
        result[algorithm] = dict()
        with open(csv_file_path) as file:
            csv_file = csv.reader(file)
            headers = next(csv_file)
            for row in csv_file:
                if totalIntermediateResultsProducedWithoutNULL in row:
                    result[algorithm][totalIntermediateResultsProducedWithoutNULL] = [to_float(num) for num in row[1:]]
                elif number_of_hash_table_probe_within_full_reducer in row:
                    result[algorithm][totalIntermediateResultsProducedWithoutNULL] = [x + y for x, y in
                                                                                      zip(result[algorithm][
                                                                                              totalIntermediateResultsProducedWithoutNULL],
                                                                                          [to_float(num) for num in
                                                                                           row[1:]])]
    return result


def plot_linear(algorithm_pair):
    ssb_conf = {
        DATA_SOURCE_CSV: {TTJ: SSB_TTJ_AGG_STATS,
                          HJ: SSB_HJ_AGG_STATS,
                          Yannakakis1Pass: SSB_Yannakakis1Pass_AGG_STATS}
    }
    ssb_data = extract_data_from_agg_csv(ssb_conf[DATA_SOURCE_CSV], get_ssb_stats_path_linear)

    tpch_conf = {
        DATA_SOURCE_CSV: {TTJ: TPCH_TTJ_AGG_STATS,
                          HJ: TPCH_HJ_AGG_STATS,
                          Yannakakis1Pass: TPCH_Yannakakis1Pass_AGG_STATS}
    }
    tpch_data = cleanup_csv(tpch_conf[DATA_SOURCE_CSV], get_tpch_stats_path_linear)

    job_conf = {
        DATA_SOURCE_CSV: {TTJ: JOB_TTJ_AGG_STATS,
                          HJ: JOB_HJ_AGG_STATS,
                          Yannakakis1Pass: JOB_Yannakakis1Pass_AGG_STATS}
    }
    job_data = extract_data_from_agg_csv(job_conf[DATA_SOURCE_CSV], get_job_stats_path_linear)

    # Add JOB data
    job_x_axis_algorithm_time = [speed / 1000 for speed in
                                 job_data[algorithm_pair.x_axis_algorithm][
                                     totalIntermediateResultsProducedWithoutNULL]]
    job_y_axis_algorithm_time = [speed / 1000 for speed in
                                 job_data[algorithm_pair.y_axis_algorithm][
                                     totalIntermediateResultsProducedWithoutNULL]]

    # Add TPC-H data
    tpch_x_axis_algorithm_time = [speed / 1000 for speed in
                                  tpch_data[algorithm_pair.x_axis_algorithm][
                                      totalIntermediateResultsProducedWithoutNULL]]
    tpch_y_axis_algorithm_time = [speed / 1000 for speed in
                                  tpch_data[algorithm_pair.y_axis_algorithm][
                                      totalIntermediateResultsProducedWithoutNULL]]

    # Add SSB data
    ssb_x_axis_algorithm_time = [speed / 1000 for speed in
                                 ssb_data[algorithm_pair.x_axis_algorithm][totalIntermediateResultsProducedWithoutNULL]]
    ssb_y_axis_algorithm_time = [speed / 1000 for speed in
                                 ssb_data[algorithm_pair.y_axis_algorithm][totalIntermediateResultsProducedWithoutNULL]]
    # 
    # Create a scatter plot
    f, ax = plt.subplots(figsize=(6, 6))
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_aspect('equal', adjustable='box')

    axis_label_font_size = 20

    if algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(1, 3 * pow(10, 4))
        plt.ylim(1, 3 * pow(10, 4))
        plt.xlabel(r"$\mathsf{HJ}$ # of hash table probe", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}^{OPT}$ # of hash table probe", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR, label="TPC-H")
        plt.scatter(ssb_x_axis_algorithm_time, ssb_y_axis_algorithm_time, s=20, marker="x", color=SSB_COLOR,
                    label="SSB")
    elif algorithm_pair.x_axis_algorithm == Yannakakis1Pass and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(1, 3 * pow(10, 5))
        plt.ylim(1, 3 * pow(10, 5))
        plt.xlabel(r"$\mathsf{YA}$ # of hash table probe", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}^{OPT}$ # of hash table probe", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR,
                    label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR, label="TPC-H")
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

    # plt.tight_layout(rect=[0, 0, 0.85, 1])  # Shrink plot area to 85% width (leaves space for legend)
    plt.tight_layout()
    if algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("mega-hashprobe-ttj-opt-hj.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == Yannakakis1Pass and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("mega-hashprobe-ttj-opt-ya.pdf", format='pdf')


if __name__ == "__main__":
    plot_linear(AlgorithmPair(TTJ, HJ))
    plot_linear(AlgorithmPair(TTJ, Yannakakis1Pass))
