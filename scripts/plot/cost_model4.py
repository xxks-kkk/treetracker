"""
Plot data based on cost model 4
"""
import csv
import json
from decimal import Decimal
from pathlib import Path
from typing import List

import matplotlib
from adjustText import adjust_text
from matplotlib import pyplot as plt
from seaborn import regplot

from plot.constants import FIG_SAVE_LOCATION, DATA_SOURCE_CSV, HJ, TTJ, LIP, Yannakakis, JOB_RELATION_SIZE, TTJ_NO_NG, \
    TTJ_BF, TTJ_BG, YannakakisB, Yannakakis1Pass, TTJ_NO_DP, TTJ_VANILLA
from plot.cost import JSON_PREFIX, AGG_STATS_FIELDS, DATA_PATH, PATTERNS, FILENAME_SIGNATURE, COST, get_query_name
from plot.job import construct_fig_name
from plot.utility import to_float, render_filename, check_argument
from precondition import precondition

TTJ_JOB_COST_MODEL4 = {
    JSON_PREFIX: "TTJHPSTATS_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['cost estimatation (cost model 4 weak assumption)'],
    DATA_PATH: r"../../results/others/cost-model4",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model4/img/",
    DATA_SOURCE_CSV: "JOB-Performance-Production-Run-overview.csv",
    PATTERNS: ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8c', 'q9', 'q10', 'q11',
               'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21',
               'q22', 'q23', 'q25', 'q26', 'q27', 'q28', 'q30', 'q31', 'q32', 'q33'],
    FILENAME_SIGNATURE: "job",
    COST: 'cost estimatation (cost model 4 weak assumption)'
}

Yannakakis_JOB_COST_MODEL4 = {
    JSON_PREFIX: "Yannakakis_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['cost estimatation (cost model 4 weak assumption)'],
    DATA_PATH: r"../../results/others/cost-model4",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model4/img/",
    DATA_SOURCE_CSV: "JOB-Performance-Production-Run-overview.csv",
    PATTERNS: ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8c', 'q9', 'q10', 'q11',
               'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21',
               'q22', 'q23', 'q25', 'q26', 'q27', 'q28', 'q30', 'q31', 'q32', 'q33'],
    FILENAME_SIGNATURE: "job",
    COST: 'cost estimatation (cost model 4 weak assumption)'
}


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
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[HJ][query_label.lower()] = to_float(num_str)
            elif TTJ in row:
                result[TTJ] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[TTJ][query_label.lower()] = to_float(num_str)
            elif TTJ_NO_DP in row:
                result[TTJ_NO_DP] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx], row[start_idx:end_idx]):
                    result[TTJ_NO_DP][query_label.lower()] = to_float(num_str)
            elif TTJ_VANILLA in row:
                result[TTJ_VANILLA] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx], row[start_idx:end_idx]):
                    result[TTJ_VANILLA][query_label.lower()] = to_float(num_str)
            elif LIP in row:
                result[LIP] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[LIP][query_label.lower()] = to_float(num_str)
            elif Yannakakis in row:
                result[Yannakakis] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[Yannakakis][query_label.lower()] = to_float(num_str)
            elif Yannakakis1Pass in row:
                result[Yannakakis1Pass] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[Yannakakis1Pass][query_label.lower()] = to_float(num_str)
            elif YannakakisB in row:
                result[YannakakisB] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[YannakakisB][query_label.lower()] = to_float(num_str)
            elif TTJ_NO_NG in row:
                result[TTJ_NO_NG] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[TTJ_NO_NG][query_label.lower()] = to_float(num_str)
            elif TTJ_BF in row:
                result[TTJ_BF] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[TTJ_BF][query_label.lower()] = to_float(num_str)
            elif TTJ_BG in row:
                result[TTJ_BG] = dict()
                for query_label, num_str in zip(headers[start_idx:end_idx],row[start_idx:end_idx]):
                    result[TTJ_BG][query_label.lower()] = to_float(num_str)
    return result


def gather_data(conf: dict) -> dict:
    result = dict()
    data = extract_data_from_csv(Path(conf[DATA_SOURCE_CSV]), column_range=[1, 34])
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
            agg_dict_new['runtime (ms)'] = data[algorithm][query_label] if query_label != 'q8c' else data[algorithm]['q8']
            result[query_label] = agg_dict_new
    return result


def plot_dot_plots(ttj_conf: dict, yannakakis_conf: dict, enable_log_scale):
    @precondition(lambda ttj_job_costs, yannakakis_job_costs: len(ttj_job_costs) == len(yannakakis_job_costs),
                  "input costs have different length")
    def compute_ratio(ttj_job_costs: dict, yannakakis_job_costs: dict):
        result = []
        for pattern in ttj_conf[PATTERNS]:
            ttj_agg_dict = ttj_job_costs[pattern]
            yannakakis_agg_dict = yannakakis_job_costs[pattern]
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
    texts = []
    for i in range(len(x)):
        label = labels[i] if labels[i] != 'q8c' else 'q8'
        texts.append(plt.text(x[i], y[i], label))
    plt.scatter(x, y)
    if enable_log_scale:
        regplot(x=x, y=y, fit_reg=True, scatter=True, logx=True)
    else:
        regplot(x=x, y=y, fit_reg=True, scatter=True)
    adjust_text(texts, expand_points=(2, 2),
                arrowprops=dict(arrowstyle="-", color='k', lw=0.5))
    if enable_log_scale:
        filename = construct_fig_name(ttj_conf, f"true_cost_ratio_{ttj_conf[FILENAME_SIGNATURE]}_log.pdf")
    else:
        filename = construct_fig_name(ttj_conf, f"true_cost_ratio_{ttj_conf[FILENAME_SIGNATURE]}.pdf")
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()


def plot_dot_plots_prop5(ttj_conf: dict,
                         yannakakis_conf: dict,
                         enable_log_scale,
                         use_full_reducer_lower_bound,
                         times_num_relations_to_output_size):
    """
    We use the actual join output size instead of the cross product upper bound to compute the total cost and
    then compute the ratio.
    """
    def get_num_relations(pattern):
        other_data_path = r"../../results/others/simple-cost-model"
        data_path_abs = Path(other_data_path).resolve()
        pattern_for_path = pattern if pattern != 'q8' else 'q8c'
        target_file_paths = [f for f in
                             data_path_abs.glob(
                                 Yannakakis_JOB_COST_MODEL4[JSON_PREFIX] + "." + pattern_for_path + "." + "*.json")]
        check_argument(len(target_file_paths) == 1, f"{target_file_paths} more than 1")
        with open(target_file_paths[0], 'r') as json_file:
            data_dict = json.load(json_file)
            agg_dict = data_dict['Aggregation Stats']
            num_relations = agg_dict['numRelations']
            return num_relations

    def find_cross_product_size(pattern):
        """
        Find out the join output size used by cross product for the given pattern.
        """
        other_data_path = r"../../results/others/simple-cost-model"
        data_path_abs = Path(other_data_path).resolve()
        pattern_for_path = pattern if pattern != 'q8' else 'q8c'
        target_file_paths = [f for f in
                             data_path_abs.glob(Yannakakis_JOB_COST_MODEL4[JSON_PREFIX] + "." + pattern_for_path + "." + "*.json")]
        check_argument(len(target_file_paths) == 1, f"{target_file_paths} more than 1")
        cross_product = Decimal(1)
        with open(target_file_paths[0], 'r') as json_file:
            data_dict = json.load(json_file)
            agg_dict = data_dict['Aggregation Stats']
            num_relations = agg_dict['numRelations']
            for key in data_dict:
                if 'full reducer' in key:
                    full_reducer = data_dict[key]
                    tuples_removed_by_full_reducer = full_reducer['numberOfTuplesRemovedByFullReducer']
                    for relation, tuple_removed in tuples_removed_by_full_reducer.items():
                        cross_product *= (Decimal(JOB_RELATION_SIZE[relation]) - Decimal(tuple_removed))
                    return cross_product * Decimal(num_relations-1)

    def setup_cost_ttj(algo_job_costs: dict, algo_conf: dict):
        """
        We have to recover TTJ cleaning cost, and get join output size based on other data.
        """
        other_data_path = r"../../results/others/simple-cost-model"
        for pattern in ttj_conf[PATTERNS]:
            algo_agg_dict = algo_job_costs[pattern]
            data_path_abs = Path(other_data_path).resolve()
            pattern_for_path = pattern if pattern != 'q8' else 'q8c'
            target_file_paths = [f for f in data_path_abs.glob(algo_conf[JSON_PREFIX] + "." + pattern_for_path + "." + "*.json")]
            check_argument(len(target_file_paths) == 1, f"{target_file_paths} more than 1")
            with open(target_file_paths[0], 'r') as json_file:
                data_dict = json.load(json_file)
                agg_dict = data_dict['Aggregation Stats']
                algo_agg_dict['resutSetSize'] = agg_dict['resutSetSize']
                if algo_agg_dict['resutSetSize'] == 0:
                    print(pattern)
                # In the current ./../results/others/cost-model4, we use algo_agg_dict[algo_conf[COST]] to
                # store the upper bound of TTJ cleaning cost
                algo_agg_dict['TTJCleaningCostUpperBound'] = algo_agg_dict[algo_conf[COST]]
                check_argument(algo_agg_dict['TTJCleaningCostUpperBound'] > 0, f"{algo_agg_dict['TTJCleaningCostUpperBound']}")
                if times_num_relations_to_output_size:
                    algo_agg_dict['TTJNewCostEstimate'] = algo_agg_dict['TTJCleaningCostUpperBound'] + \
                                                          algo_agg_dict['resutSetSize'] * (get_num_relations(pattern_for_path) - 1)
                else:
                    algo_agg_dict['TTJNewCostEstimate'] = algo_agg_dict['TTJCleaningCostUpperBound'] + \
                                                          algo_agg_dict['resutSetSize']

    def setup_cost_yannakakis(algo_job_costs: dict, algo_conf: dict):
        """
        We have to recover full reducer cost, and get join output size based on other data.
        """
        def get_full_reducer_cost(pattern):
            data_path_abs = Path(other_data_path).resolve()
            pattern_for_path = pattern if pattern != 'q8' else 'q8c'
            target_file_paths = [f for f in
                                 data_path_abs.glob(
                                     Yannakakis_JOB_COST_MODEL4[JSON_PREFIX] + "." + pattern_for_path + "." + "*.json")]
            check_argument(len(target_file_paths) == 1, f"{target_file_paths} more than 1")
            with open(target_file_paths[0], 'r') as json_file:
                data_dict = json.load(json_file)
                for key in data_dict:
                    if 'full reducer' in key:
                        full_reducer = data_dict[key]
                        return Decimal(full_reducer['numberOfR1Assignments'])

        other_data_path = r"../../results/others/simple-cost-model"
        for pattern in ttj_conf[PATTERNS]:
            algo_agg_dict = algo_job_costs[pattern]
            data_path_abs = Path(other_data_path).resolve()
            pattern_for_path = pattern if pattern != 'q8' else 'q8c'
            target_file_paths = [f for f in data_path_abs.glob(algo_conf[JSON_PREFIX] + "." + pattern_for_path + "." + "*.json")]
            check_argument(len(target_file_paths) == 1, f"{target_file_paths} more than 1")
            with open(target_file_paths[0], 'r') as json_file:
                data_dict = json.load(json_file)
                agg_dict = data_dict['Aggregation Stats']
                algo_agg_dict['resutSetSize'] = agg_dict['resutSetSize']
                if algo_agg_dict['resutSetSize'] == 0:
                    print(pattern)
                algo_agg_dict['crossProductUpperBound'] = find_cross_product_size(pattern_for_path)
                # In the current ./../results/others/cost-model4, we use algo_agg_dict[algo_conf[COST]] to
                # store the lower bound of full reducer
                algo_agg_dict['fullReducerCostLowerBound'] = algo_agg_dict[algo_conf[COST]]
                algo_agg_dict['fullReducerCost'] = get_full_reducer_cost(pattern_for_path)
                algo_agg_dict['YannakakisCostLowerBound'] = algo_agg_dict['fullReducerCostLowerBound'] + algo_agg_dict['resutSetSize']
                check_argument(algo_agg_dict['YannakakisCostLowerBound'] > 0, f"{algo_agg_dict['YannakakisCostLowerBound']}")
                if times_num_relations_to_output_size:
                    algo_agg_dict['YannakakisCost'] = algo_agg_dict['fullReducerCost'] + \
                                                      algo_agg_dict['resutSetSize'] * (get_num_relations(pattern_for_path) - 1)
                else:
                    algo_agg_dict['YannakakisCost'] = algo_agg_dict['fullReducerCost'] + algo_agg_dict['resutSetSize']

    @precondition(lambda ttj_job_costs, yannakakis_job_costs, ttj_cost_key, yannakakis_cost_key:
                  len(ttj_job_costs) == len(yannakakis_job_costs),
                  "input costs have different length")
    def compute_ratio(ttj_job_costs: dict, yannakakis_job_costs: dict, ttj_cost_key, yannakakis_cost_key):
        result = []
        for pattern in ttj_conf[PATTERNS]:
            ttj_agg_dict = ttj_job_costs[pattern]
            yannakakis_agg_dict = yannakakis_job_costs[pattern]
            cost_ratio = yannakakis_agg_dict[yannakakis_cost_key] / ttj_agg_dict[ttj_cost_key]
            runtime_ratio = yannakakis_agg_dict['runtime (ms)'] / ttj_agg_dict['runtime (ms)']
            if cost_ratio > 1 and runtime_ratio > 1:
                print(f"cost_ratio > 1 & runtime_ratio > 1: {pattern}")
            if cost_ratio > 1 and runtime_ratio < 1:
                print(f"cost_ratio > 1 & runtime_ratio < 1: {pattern}")
            result.append({"query": pattern, "cost_ratio": cost_ratio, "runtime_ratio": runtime_ratio})
        return result

    ttj_job_costs = gather_data(ttj_conf)
    setup_cost_ttj(ttj_job_costs, ttj_conf)
    yannakakis_job_costs = gather_data(yannakakis_conf)
    setup_cost_yannakakis(yannakakis_job_costs, yannakakis_conf)
    if use_full_reducer_lower_bound:
        result = compute_ratio(ttj_job_costs, yannakakis_job_costs, 'TTJNewCostEstimate', 'YannakakisCostLowerBound')
    else:
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
    texts = []
    for i in range(len(x)):
        label = labels[i] if labels[i] != 'q8c' else 'q8'
        texts.append(plt.text(x[i], y[i], label))
    plt.scatter(x, y)
    adjust_text(texts, expand_points=(2, 2),
                arrowprops=dict(arrowstyle="-", color='k', lw=0.5))
    filename = construct_fig_name(ttj_conf,
                                  render_filename(f"true_cost_ratio_{ttj_conf[FILENAME_SIGNATURE]}",
                                                  enable_log_scale=enable_log_scale,
                                                  use_full_reducer_lower_bound=use_full_reducer_lower_bound,
                                                  times_num_relations_to_output_size=times_num_relations_to_output_size))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()


def plot_dots_that_ttj_cost_smaller_than_y_cost(ttj_conf: dict,
                                                yannakakis_conf: dict,
                                                enable_log_scale,
                                                focus_dp_with_cost_diff_greater_than_zero):

    @precondition(lambda ttj_job_costs, yannakakis_job_costs: len(ttj_job_costs) == len(yannakakis_job_costs),
                  "input costs have different length")
    def gather_target_dp(ttj_job_costs: dict, yannakakis_job_costs: dict):
        result = []
        for pattern in ttj_conf[PATTERNS]:
            ttj_agg_dict = ttj_job_costs[pattern]
            yannakakis_agg_dict = yannakakis_job_costs[pattern]
            cost_diff = yannakakis_agg_dict[yannakakis_conf[COST]] - ttj_agg_dict[ttj_conf[COST]]
            runtime_diff = yannakakis_agg_dict['runtime (ms)'] - ttj_agg_dict['runtime (ms)']
            if (focus_dp_with_cost_diff_greater_than_zero and cost_diff > 0) or \
                    not focus_dp_with_cost_diff_greater_than_zero:
                result.append({"query": pattern, "cost_diff": cost_diff, "runtime_diff": runtime_diff})
        return result

    ttj_job_costs = gather_data(ttj_conf)
    yannakakis_job_costs = gather_data(yannakakis_conf)
    result = gather_target_dp(ttj_job_costs, yannakakis_job_costs)
    x = [d['cost_diff'] for d in result]
    y = [d['runtime_diff'] for d in result]
    labels = [d['query'] for d in result]
    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)
    if enable_log_scale:
        ax.set_xscale('log', base=10)
        ax.set_yscale('log', base=10)
    ax.minorticks_off()
    if not enable_log_scale:
        plt.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))
    plt.grid(which='major', axis='both', zorder=-1.0)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    if enable_log_scale:
        plt.ylabel('Yannakakis runtime - TTJ runtime in log scale', fontdict=font2)
    else:
        plt.ylabel('Yannakakis runtime - TTJ runtime', fontdict=font2)
    if enable_log_scale:
        plt.xlabel('Yannakakis cost - TTJ cost in log scale', fontdict=font2)
    else:
        plt.xlabel('Yannakakis cost - TTJ cost', fontdict=font2)
    texts = []
    for i in range(len(x)):
        label = labels[i] if labels[i] != 'q8c' else 'q8'
        texts.append(plt.text(x[i], y[i], label))
    plt.scatter(x, y)
    adjust_text(texts, expand_points=(2, 2),
                arrowprops=dict(arrowstyle="-", color='k', lw=0.5))
    filename = construct_fig_name(ttj_conf, render_filename(f"true_cost_diff_{ttj_conf[FILENAME_SIGNATURE]}", enable_log_scale, focus_dp_with_cost_diff_greater_than_zero))
    plt.savefig(filename, format='pdf')
    plt.show()

if __name__ == "__main__":
    # plot_dot_plots(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, False)
    # plot_dot_plots(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, True)
    plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, False, False)
    plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, True, False)
    plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, False, True)
    plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, True, True)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, False, True, False)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, True, True, False)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, False, True, True)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, True, True, True)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, False, False, False)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, True, False, False)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, False, False, True)
    # plot_dot_plots_prop5(TTJ_JOB_COST_MODEL4, Yannakakis_JOB_COST_MODEL4, True, False, True)