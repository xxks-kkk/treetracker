"""
Plot data based on cost model 4 on JOB with predicates
"""
import csv
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import List, Any

import matplotlib
from matplotlib import pyplot as plt

from plot.constants import FIG_SAVE_LOCATION, DATA_SOURCE_CSV, HJ, TTJ, LIP, Yannakakis
from plot.cost import JSON_PREFIX, AGG_STATS_FIELDS, DATA_PATH, PATTERNS, FILENAME_SIGNATURE, COST
from plot.job import construct_fig_name
from plot.utility import to_float, render_filename, check_argument
from precondition import precondition

TTJ_JOB_COST_MODEL4_WITH_PREDICATES = {
    JSON_PREFIX: "TTJHPSTATS_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['cost estimatation (cost model 4 weak assumption)'],
    DATA_PATH: r"../../results/others/cost-model4-with-predicates",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model4-with-predicates/img/",
    DATA_SOURCE_CSV: "JOB-Performance-Summary-(with-predicates).csv",
    PATTERNS: ["Query1a", "Query1b", "Query1c", "Query1d",
               "Query2a", "Query2b", "Query2c", "Query2d",
               "Query3a", "Query3b", "Query3c",
               "Query4a", "Query4b", "Query4c",
               "Query5a", "Query5b", "Query5c",
               "Query6a", "Query6b", "Query6c", "Query6d", "Query6e", "Query6f",
               "Query7a", "Query7b", "Query7c",
               "Query8a", "Query8b", "Query8c", "Query8d",
               "Query9a", "Query9b", "Query9c", "Query9d",
               "Query10a", "Query10b", "Query10c",
               "Query11a", "Query11b", "Query11c", "Query11d",
               "Query12a", "Query12b", "Query12c",
               "Query13a", "Query13b", "Query13c", "Query13d",
               "Query14a", "Query14b", "Query14c",
               "Query15a", "Query15b", "Query15c", "Query15d",
               "Query16a", "Query16b", "Query16c", "Query16d",
               "Query17a", "Query17b", "Query17c", "Query17d", "Query17e", "Query17f",
               "Query18a", "Query18b", "Query18c",
               "Query19a", "Query19b", "Query19c", "Query19d",
               "Query20a", "Query20b", "Query20c",
               "Query21a", "Query21b", "Query21c",
               "Query22a", "Query22b", "Query22c", "Query22d",
               "Query23a", "Query23b", "Query23c",
               "Query24a", "Query24b",
               "Query25a", "Query25b", "Query25c",
               "Query26a", "Query26b", "Query26c",
               "Query27a", "Query27b", "Query27c",
               "Query28a", "Query28b", "Query28c",
               "Query29a", "Query29b", "Query29c",
               "Query30a", "Query30b", "Query30c",
               "Query31a", "Query31b", "Query31c",
               "Query32a", "Query32b",
               "Query33a", "Query33b", "Query33c"],
    FILENAME_SIGNATURE: "job",
    COST: 'cost estimatation (cost model 4 weak assumption)'
}

Yannakakis_JOB_COST_MODEL4_WITH_PREDICATES = {
    JSON_PREFIX: "Yannakakis_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['cost estimatation (cost model 4 weak assumption)'],
    DATA_PATH: r"../../results/others/cost-model4-with-predicates",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model4-with-predicates/img/",
    DATA_SOURCE_CSV: "JOB-Performance-Summary-(with-predicates).csv",
    PATTERNS: ["Query1a", "Query1b", "Query1c", "Query1d",
               "Query2a", "Query2b", "Query2c", "Query2d",
               "Query3a", "Query3b", "Query3c",
               "Query4a", "Query4b", "Query4c",
               "Query5a", "Query5b", "Query5c",
               "Query6a", "Query6b", "Query6c", "Query6d", "Query6e", "Query6f",
               "Query7a", "Query7b", "Query7c",
               "Query8a", "Query8b", "Query8c", "Query8d",
               "Query9a", "Query9b", "Query9c", "Query9d",
               "Query10a", "Query10b", "Query10c",
               "Query11a", "Query11b", "Query11c", "Query11d",
               "Query12a", "Query12b", "Query12c",
               "Query13a", "Query13b", "Query13c", "Query13d",
               "Query14a", "Query14b", "Query14c",
               "Query15a", "Query15b", "Query15c", "Query15d",
               "Query16a", "Query16b", "Query16c", "Query16d",
               "Query17a", "Query17b", "Query17c", "Query17d", "Query17e", "Query17f",
               "Query18a", "Query18b", "Query18c",
               "Query19a", "Query19b", "Query19c", "Query19d",
               "Query20a", "Query20b", "Query20c",
               "Query21a", "Query21b", "Query21c",
               "Query22a", "Query22b", "Query22c", "Query22d",
               "Query23a", "Query23b", "Query23c",
               "Query24a", "Query24b",
               "Query25a", "Query25b", "Query25c",
               "Query26a", "Query26b", "Query26c",
               "Query27a", "Query27b", "Query27c",
               "Query28a", "Query28b", "Query28c",
               "Query29a", "Query29b", "Query29c",
               "Query30a", "Query30b", "Query30c",
               "Query31a", "Query31b", "Query31c",
               "Query32a", "Query32b",
               "Query33a", "Query33b", "Query33c"],
    FILENAME_SIGNATURE: "job",
    COST: 'cost estimatation (cost model 4 weak assumption)'
}

TTJ_JOB_SIMPLE_COST = {
    JSON_PREFIX: "TTJHPSTATS_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['simpleCostModelCost', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/simple-cost-model-with-predicates",
    FIG_SAVE_LOCATION: r"../../results/others/simple-cost-model-with-predicates/img/",
    DATA_SOURCE_CSV: "JOB-Performance-Summary-(with-predicates).csv",
    PATTERNS: ["Query1a", "Query1b", "Query1c", "Query1d",
               "Query2a", "Query2b", "Query2c", "Query2d",
               "Query3a", "Query3b", "Query3c",
               "Query4a", "Query4b", "Query4c",
               "Query5a", "Query5b", "Query5c",
               "Query6a", "Query6b", "Query6c", "Query6d", "Query6e", "Query6f",
               "Query7a", "Query7b", "Query7c",
               "Query8a", "Query8b", "Query8c", "Query8d",
               "Query9a", "Query9b", "Query9c", "Query9d",
               "Query10a", "Query10b", "Query10c",
               "Query11a", "Query11b", "Query11c", "Query11d",
               "Query12a", "Query12b", "Query12c",
               "Query13a", "Query13b", "Query13c", "Query13d",
               "Query14a", "Query14b", "Query14c",
               "Query15a", "Query15b", "Query15c", "Query15d",
               "Query16a", "Query16b", "Query16c", "Query16d",
               "Query17a", "Query17b", "Query17c", "Query17d", "Query17e", "Query17f",
               "Query18a", "Query18b", "Query18c",
               "Query19a", "Query19b", "Query19c", "Query19d",
               "Query20a", "Query20b", "Query20c",
               "Query21a", "Query21b", "Query21c",
               "Query22a", "Query22b", "Query22c", "Query22d",
               "Query23a", "Query23b", "Query23c",
               "Query24a", "Query24b",
               "Query25a", "Query25b", "Query25c",
               "Query26a", "Query26b", "Query26c",
               "Query27a", "Query27b", "Query27c",
               "Query28a", "Query28b", "Query28c",
               "Query29a", "Query29b", "Query29c",
               "Query30a", "Query30b", "Query30c",
               "Query31a", "Query31b", "Query31c",
               "Query32a", "Query32b",
               "Query33a", "Query33b", "Query33c"],
    FILENAME_SIGNATURE: "job",
    COST: 'simpleCostModelCost'
}

Yannakakis_JOB_SIMPLE_COST = {
    JSON_PREFIX: "Yannakakis_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['simpleCostModelCost', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/simple-cost-model-with-predicates",
    FIG_SAVE_LOCATION: r"../../results/others/simple-cost-model-with-predicates/img/",
    DATA_SOURCE_CSV: "JOB-Performance-Summary-(with-predicates).csv",
    PATTERNS: ["Query1a", "Query1b", "Query1c", "Query1d",
               "Query2a", "Query2b", "Query2c", "Query2d",
               "Query3a", "Query3b", "Query3c",
               "Query4a", "Query4b", "Query4c",
               "Query5a", "Query5b", "Query5c",
               "Query6a", "Query6b", "Query6c", "Query6d", "Query6e", "Query6f",
               "Query7a", "Query7b", "Query7c",
               "Query8a", "Query8b", "Query8c", "Query8d",
               "Query9a", "Query9b", "Query9c", "Query9d",
               "Query10a", "Query10b", "Query10c",
               "Query11a", "Query11b", "Query11c", "Query11d",
               "Query12a", "Query12b", "Query12c",
               "Query13a", "Query13b", "Query13c", "Query13d",
               "Query14a", "Query14b", "Query14c",
               "Query15a", "Query15b", "Query15c", "Query15d",
               "Query16a", "Query16b", "Query16c", "Query16d",
               "Query17a", "Query17b", "Query17c", "Query17d", "Query17e", "Query17f",
               "Query18a", "Query18b", "Query18c",
               "Query19a", "Query19b", "Query19c", "Query19d",
               "Query20a", "Query20b", "Query20c",
               "Query21a", "Query21b", "Query21c",
               "Query22a", "Query22b", "Query22c", "Query22d",
               "Query23a", "Query23b", "Query23c",
               "Query24a", "Query24b",
               "Query25a", "Query25b", "Query25c",
               "Query26a", "Query26b", "Query26c",
               "Query27a", "Query27b", "Query27c",
               "Query28a", "Query28b", "Query28c",
               "Query29a", "Query29b", "Query29c",
               "Query30a", "Query30b", "Query30c",
               "Query31a", "Query31b", "Query31c",
               "Query32a", "Query32b",
               "Query33a", "Query33b", "Query33c"],
    FILENAME_SIGNATURE: "job",
    COST: 'simpleCostModelCost'
}



def get_query_name(query_name: str, patterns: List[Any]) -> str:
    for pattern in patterns:
        if re.search(f"\\b{pattern}\\b", query_name) is not None:
            return pattern.replace('Query', 'q')


def extract_data_from_csv(csv_file_path: Path,
                          column_range: List[int]) -> dict:
    """
    Extract the data from csv. The result is
    {hj: [...], ttj: [...], lip: [...], yannakakis: [...]}
    """
    start_idx = column_range[0]
    end_idx = column_range[1]
    result = dict()
    with open(csv_file_path, "r") as file:
        csv_file = csv.reader(file)
        headers = next(csv_file)
        for row in csv_file:
            if HJ in row:
                result[HJ] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx], row[start_idx:end_idx]):
                    result[HJ][query_label.lower()] = to_float(num_str)
            elif TTJ in row:
                result[TTJ] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx], row[start_idx:end_idx]):
                    result[TTJ][query_label.lower()] = to_float(num_str)
            elif LIP in row:
                result[LIP] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx], row[start_idx:end_idx]):
                    result[LIP][query_label.lower()] = to_float(num_str)
            elif Yannakakis in row:
                result[Yannakakis] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx], row[start_idx:end_idx]):
                    result[Yannakakis][query_label.lower()] = to_float(num_str)
    return result


def gather_data(conf: dict) -> dict:
    result = dict()
    data = extract_data_from_csv(Path(conf[DATA_SOURCE_CSV]), column_range=[1, 114])
    data_path_abs = Path(conf[DATA_PATH]).resolve()
    target_file_paths = [f for f in data_path_abs.glob(conf[JSON_PREFIX] + "*.json")]
    if TTJ in conf[JSON_PREFIX]:
        algorithm = TTJ
    elif Yannakakis in conf[JSON_PREFIX]:
        algorithm = Yannakakis
    for json_file_path in target_file_paths:
        with open(json_file_path, 'r') as json_file:
            data_dict = json.load(json_file)
            agg_dict = data_dict['Aggregation Stats']
            agg_dict_new = dict()
            for agg_stats_field in conf[AGG_STATS_FIELDS]:
                agg_dict_new[agg_stats_field] = Decimal(agg_dict[agg_stats_field])
            query_label = get_query_name(agg_dict['queryName'], conf[PATTERNS])
            agg_dict_new['runtime (ms)'] = data[algorithm][query_label]
            result[query_label] = agg_dict_new
    return result


def plot_dot_plots_prop5_with_predicates(ttj_conf: dict,
                                         yannakakis_conf: dict,
                                         enable_log_scale):
    """
    We use the actual join output size instead of the cross product upper bound to compute the total cost and
    then compute the ratio.
    """

    def ensure_data_complete(algo_conf: dict, data_path: str):
        """
        We want to make sure all the requirement data are present for both TTJ and Yannakakis before carrying out
        further computation.
        """
        other_data_path = data_path
        missing_data = []
        for pattern in algo_conf[PATTERNS]:
            data_path_abs = Path(other_data_path).resolve()
            pattern_for_path = pattern
            target_file_paths = [f for f in
                                 data_path_abs.glob(algo_conf[JSON_PREFIX] + ".*." + pattern_for_path + ".json")]
            if len(target_file_paths) != 1:
                missing_data.append(pattern_for_path)
        return missing_data

    def setup_cost_ttj(algo_job_costs: dict, algo_conf: dict):
        """
        We have to recover TTJ cleaning cost, and get join output size based on other data.
        """
        other_data_path = r"../../results/others/simple-cost-model-with-predicates"
        for pattern in algo_conf[PATTERNS]:
            pattern_for_path = pattern.replace('Query', 'q')
            algo_agg_dict = algo_job_costs[pattern_for_path]
            data_path_abs = Path(other_data_path).resolve()
            target_file_paths = [f for f in
                                 data_path_abs.glob(algo_conf[JSON_PREFIX] + ".*." + pattern + ".json")]
            check_argument(len(target_file_paths) == 1, f"{target_file_paths} not equal to 1")
            with open(target_file_paths[0], 'r') as json_file:
                data_dict = json.load(json_file)
                agg_dict = data_dict['Aggregation Stats']
                algo_agg_dict['resutSetSize'] = agg_dict['resutSetSize']
                # In the current ./../results/others/cost-model4, we use algo_agg_dict[algo_conf[COST]] to
                # store the upper bound of TTJ cleaning cost
                algo_agg_dict['TTJCleaningCostUpperBound'] = algo_agg_dict[algo_conf[COST]]
                check_argument(algo_agg_dict['TTJCleaningCostUpperBound'] > 0,
                               f"{algo_agg_dict['TTJCleaningCostUpperBound']}")
                algo_agg_dict['TTJNewCostEstimate'] = algo_agg_dict['TTJCleaningCostUpperBound'] + \
                                                      algo_agg_dict['resutSetSize']

    def setup_cost_yannakakis(algo_job_costs: dict, algo_conf: dict):
        """
        We have to recover full reducer cost, and get join output size based on other data.
        """

        def get_full_reducer_cost(pattern):
            data_path_abs = Path(other_data_path).resolve()
            target_file_paths = [f for f in
                                 data_path_abs.glob(algo_conf[JSON_PREFIX] + ".*." + pattern + ".json")]
            check_argument(len(target_file_paths) == 1, f"{target_file_paths} not equal to 1")
            with open(target_file_paths[0], 'r') as json_file:
                data_dict = json.load(json_file)
                for key in data_dict:
                    if 'full reducer' in key:
                        full_reducer = data_dict[key]
                        return Decimal(full_reducer['numberOfR1Assignments'])

        other_data_path = r"../../results/others/simple-cost-model-with-predicates"
        for pattern in algo_conf[PATTERNS]:
            pattern_for_path = pattern.replace('Query', 'q')
            algo_agg_dict = algo_job_costs[pattern_for_path]
            data_path_abs = Path(other_data_path).resolve()
            target_file_paths = [f for f in
                                 data_path_abs.glob(algo_conf[JSON_PREFIX] + ".*." + pattern + ".json")]
            check_argument(len(target_file_paths) == 1, f"{target_file_paths} more than 1")
            with open(target_file_paths[0], 'r') as json_file:
                data_dict = json.load(json_file)
                agg_dict = data_dict['Aggregation Stats']
                algo_agg_dict['resutSetSize'] = agg_dict['resutSetSize']
                # In the current ./../results/others/cost-model4, we use algo_agg_dict[algo_conf[COST]] to
                # store the lower bound of full reducer
                algo_agg_dict['fullReducerCost'] = get_full_reducer_cost(pattern)
                check_argument(algo_agg_dict['fullReducerCost'] != None, f"algo_agg_dict['fullReducerCost'] is None for {target_file_paths[0]}")
                algo_agg_dict['YannakakisCost'] = algo_agg_dict['fullReducerCost'] + algo_agg_dict['resutSetSize']

    @precondition(lambda ttj_job_costs, yannakakis_job_costs, ttj_cost_key, yannakakis_cost_key:
                  len(ttj_job_costs) == len(yannakakis_job_costs),
                  "input costs have different length")
    def compute_ratio(ttj_job_costs: dict, yannakakis_job_costs: dict, ttj_cost_key, yannakakis_cost_key):
        result = []
        for pattern in ttj_conf[PATTERNS]:
            ttj_agg_dict = ttj_job_costs[pattern.replace('Query', 'q')]
            yannakakis_agg_dict = yannakakis_job_costs[pattern.replace('Query', 'q')]
            cost_ratio = yannakakis_agg_dict[yannakakis_cost_key] / ttj_agg_dict[ttj_cost_key]
            runtime_ratio = yannakakis_agg_dict['runtime (ms)'] / ttj_agg_dict['runtime (ms)']
            if cost_ratio > 1 and runtime_ratio > 1:
                print(f"cost_ratio > 1 & runtime_ratio > 1: {pattern}")
            if cost_ratio > 1 and runtime_ratio < 1:
                print(f"cost_ratio > 1 & runtime_ratio < 1: {pattern}")
            result.append({"query": pattern, "cost_ratio": cost_ratio, "runtime_ratio": runtime_ratio})
        return result

    ttj_missing_data = ensure_data_complete(ttj_conf, r"../../results/others/simple-cost-model-with-predicates")
    yannakakis_missing_data = ensure_data_complete(yannakakis_conf, r"../../results/others/simple-cost-model-with-predicates")
    check_argument(len(ttj_missing_data) == 0 and len(yannakakis_missing_data) == 0,
                   f"ttj_missing_data: {ttj_missing_data}\nyannakakis_missing_data: {yannakakis_missing_data}\nunder ../../results/others")

    ttj_missing_data = ensure_data_complete(ttj_conf, ttj_conf[DATA_PATH])
    yannakakis_missing_data = ensure_data_complete(yannakakis_conf, ttj_conf[DATA_PATH])
    check_argument(len(ttj_missing_data) == 0 and len(yannakakis_missing_data) == 0,
                   f"ttj_missing_data: {ttj_missing_data}\nyannakakis_missing_data: {yannakakis_missing_data}\nunder {ttj_conf[DATA_PATH]}")

    ttj_job_costs = gather_data(ttj_conf)
    setup_cost_ttj(ttj_job_costs, ttj_conf)
    yannakakis_job_costs = gather_data(yannakakis_conf)
    setup_cost_yannakakis(yannakakis_job_costs, yannakakis_conf)
    result = compute_ratio(ttj_job_costs, yannakakis_job_costs, 'TTJNewCostEstimate', 'YannakakisCost')
    x = [d['cost_ratio'] for d in result]
    y = [d['runtime_ratio'] for d in result]
    labels = [d['query'] for d in result]
    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)
    if enable_log_scale:
        ax.set_xscale('log', base=10)
    ax.minorticks_off()
    plt.grid(which='major', axis='both', zorder=-1.0)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    plt.ylabel('Yannakakis runtime / TTJ runtime', fontdict=font2)
    if enable_log_scale:
        plt.xlabel('Yannakakis cost / TTJ cost in log scale', fontdict=font2)
    else:
        plt.xlabel('Yannakakis cost / TTJ cost', fontdict=font2)
    plt.axhline(y=1, color='#FF0000', linestyle='-')
    plt.axvline(x=1, color='#FF0000', linestyle='-')
    plt.scatter(x, y)
    filename = construct_fig_name(ttj_conf,
                                  render_filename(f"true_cost_ratio_with_predicates_{ttj_conf[FILENAME_SIGNATURE]}",
                                                  enable_log_scale=enable_log_scale))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()


def plot_dot_plots(ttj_conf: dict, yannakakis_conf: dict):
    @precondition(lambda ttj_job_costs, yannakakis_job_costs: len(ttj_job_costs) == len(yannakakis_job_costs),
                  "input costs have different length")
    def compute_ratio(ttj_job_costs: dict, yannakakis_job_costs: dict):
        result = []
        for pattern in ttj_conf[PATTERNS]:
            ttj_agg_dict = ttj_job_costs[pattern.replace('Query', 'q')]
            yannakakis_agg_dict = yannakakis_job_costs[pattern.replace('Query', 'q')]
            cost_ratio = yannakakis_agg_dict[yannakakis_conf[COST]] / ttj_agg_dict[ttj_conf[COST]]
            runtime_ratio = yannakakis_agg_dict['runtime (ms)'] / ttj_agg_dict['runtime (ms)']
            result.append({"query": pattern, "cost_ratio": cost_ratio, "runtime_ratio": runtime_ratio})
        return result

    ttj_job_costs = gather_data(ttj_conf)
    yannakakis_job_costs = gather_data(yannakakis_conf)
    result = compute_ratio(ttj_job_costs, yannakakis_job_costs)
    x = [d['cost_ratio'] for d in result]
    y = [d['runtime_ratio'] for d in result]
    labels = [d['query'] for d in result]
    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)
    ax.minorticks_off()
    plt.grid(which='major', axis='both', zorder=-1.0)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    plt.ylabel('Yannakakis runtime / TTJ runtime', fontdict=font2)
    plt.xlabel('Yannakakis cost / TTJ cost', fontdict=font2)
    plt.axhline(y=1, color='#FF0000', linestyle='-')
    plt.axvline(x=1, color='#FF0000', linestyle='-')
    plt.scatter(x, y)
    filename = construct_fig_name(ttj_conf, render_filename(f"true_cost_ratio_with_predicates_{ttj_conf[FILENAME_SIGNATURE]}"))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    plot_dot_plots_prop5_with_predicates(TTJ_JOB_COST_MODEL4_WITH_PREDICATES, Yannakakis_JOB_COST_MODEL4_WITH_PREDICATES,
                                         False)
    plot_dot_plots(TTJ_JOB_SIMPLE_COST, Yannakakis_JOB_SIMPLE_COST)
