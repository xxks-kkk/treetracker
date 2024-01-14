"""
Processing json generated from createStatisticsJson
"""
import json
import re
from pathlib import Path
from typing import Any, List

import matplotlib
from adjustText import adjust_text
from matplotlib import pyplot as plt
from seaborn import regplot

from plot.constants import FIG_SAVE_LOCATION
from plot.job import construct_fig_name
from plot.utility import render_filename
from precondition import precondition

JSON_PREFIX = "JSON_PREFIX"
AGG_STATS_FIELDS = "AGG_STATS_FIELDS"
DATA_PATH = "DATA_PATH"
PATTERNS = "PATTERNS"
FILENAME_SIGNATURE = "FILENAME_SIGNATURE"
COST = "COST"

TTJ_JOB_COST = {
    JSON_PREFIX: "TTJHPSTATS_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['estimateCostOfTTJ', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/cost-model3",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model3/img/",
    PATTERNS: ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8c', 'q9', 'q10', 'q11',
               'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21',
               'q22', 'q23', 'q25', 'q26', 'q27', 'q28', 'q30', 'q31', 'q32', 'q33'],
    FILENAME_SIGNATURE: "job",
    COST: 'estimateCostOfTTJ'
}

Yannakakis_JOB_COST = {
    JSON_PREFIX: "Yannakakis_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['CostOfYannakakis', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/cost-model3",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model3/img/",
    PATTERNS: ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8c', 'q9', 'q10', 'q11',
               'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21',
               'q22', 'q23', 'q25', 'q26', 'q27', 'q28', 'q30', 'q31', 'q32', 'q33'],
    FILENAME_SIGNATURE: "job",
    COST: 'CostOfYannakakis'
}

TTJ_JOB_SIMPLE_COST = {
    JSON_PREFIX: "TTJHPSTATS_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['simpleCostModelCost', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/simple-cost-model",
    FIG_SAVE_LOCATION: r"../../results/others/simple-cost-model/img/",
    PATTERNS: ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8c', 'q9', 'q10', 'q11',
               'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21',
               'q22', 'q23', 'q25', 'q26', 'q27', 'q28', 'q30', 'q31', 'q32', 'q33'],
    FILENAME_SIGNATURE: "job",
    COST: 'simpleCostModelCost'
}

Yannakakis_JOB_SIMPLE_COST = {
    JSON_PREFIX: "Yannakakis_org.zhu45.treetracker.benchmark.job",
    AGG_STATS_FIELDS: ['simpleCostModelCost', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/simple-cost-model",
    FIG_SAVE_LOCATION: r"../../results/others/simple-cost-model/img/",
    PATTERNS: ['q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8c', 'q9', 'q10', 'q11',
               'q12', 'q13', 'q14', 'q15', 'q16', 'q17', 'q18', 'q19', 'q20', 'q21',
               'q22', 'q23', 'q25', 'q26', 'q27', 'q28', 'q30', 'q31', 'q32', 'q33'],
    FILENAME_SIGNATURE: "job",
    COST: 'simpleCostModelCost'
}

TTJ_BACKJUMP_COST = {
    JSON_PREFIX: "TTJHPSTATS_BackJumpQuery",
    AGG_STATS_FIELDS: ['estimateCostOfTTJ', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/cost-model3/backjump",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model3/backjump/img",
    PATTERNS: ['BackJumpQuery_1024_2', 'BackJumpQuery_16384_2', 'BackJumpQuery_2_1', 'BackJumpQuery_2_16',
               'BackJumpQuery_2_2', 'BackJumpQuery_2_32', 'BackJumpQuery_2_4', 'BackJumpQuery_256_2',
               'BackJumpQuery_2_8', 'BackJumpQuery_4096_2', 'BackJumpQuery_65536_2'],
    FILENAME_SIGNATURE: "backjump",
    COST: 'estimateCostOfTTJ'
}

Yannakakis_BACKJUMP_COST = {
    JSON_PREFIX: "Yannakakis_BackJumpQuery",
    AGG_STATS_FIELDS: ['CostOfYannakakis', 'runtime (ms)'],
    DATA_PATH: r"../../results/others/cost-model3/backjump",
    FIG_SAVE_LOCATION: r"../../results/others/cost-model3/backjump/img",
    PATTERNS: ['BackJumpQuery_1024_2', 'BackJumpQuery_16384_2', 'BackJumpQuery_2_1', 'BackJumpQuery_2_16',
               'BackJumpQuery_2_2', 'BackJumpQuery_2_32', 'BackJumpQuery_2_4', 'BackJumpQuery_256_2',
               'BackJumpQuery_2_8', 'BackJumpQuery_4096_2', 'BackJumpQuery_65536_2'],
    FILENAME_SIGNATURE: "backjump",
    COST: 'CostOfYannakakis'
}


def get_query_name(query_name: str, patterns: List[Any]) -> str:
    for pattern in patterns:
        if re.search(f"\\b{pattern}\\b", query_name) is not None:
            return pattern


def gather_data(conf: dict) -> dict:
    result = dict()
    data_path_abs = Path(conf[DATA_PATH]).resolve()
    target_file_paths = [f for f in data_path_abs.glob(conf[JSON_PREFIX] + "*.json")]
    for json_file_path in target_file_paths:
        with open(json_file_path, 'r') as json_file:
            data_dict = json.load(json_file)
            agg_dict = data_dict['Aggregation Stats']
            agg_dict_new = dict()
            for agg_stats_field in conf[AGG_STATS_FIELDS]:
                agg_dict_new[agg_stats_field] = agg_dict[agg_stats_field]
                # include final join output size to the cost model
                if agg_stats_field == conf[COST]:
                    agg_dict_new[agg_stats_field] = agg_dict[agg_stats_field]
                    agg_dict_new[agg_stats_field] += agg_dict['resutSetSize']
            result[get_query_name(agg_dict['queryName'], conf[PATTERNS])] = agg_dict_new
    return result


def plot_dot_plots(ttj_conf: dict, yannakakis_conf: dict, enable_log_scale, enable_linear_regression):
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
    if enable_linear_regression:
        if enable_log_scale:
            regplot(x=x, y=y, fit_reg=True, scatter=True, logx=True)
        else:
            regplot(x=x, y=y, fit_reg=True, scatter=True)
    adjust_text(texts, expand_points=(2, 2),
                arrowprops=dict(arrowstyle="-", color='k', lw=0.5))
    filename = construct_fig_name(ttj_conf, render_filename(f"true_cost_ratio_{ttj_conf[FILENAME_SIGNATURE]}",
                                                            enable_log_scale=enable_log_scale,
                                                            enable_linear_regression=enable_linear_regression))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()


def plot_dots_that_ttj_cost_smaller_than_y_cost(ttj_conf: dict, yannakakis_conf: dict, enable_log_scale):
    @precondition(lambda ttj_job_costs, yannakakis_job_costs: len(ttj_job_costs) == len(yannakakis_job_costs),
                  "input costs have different length")
    def gather_target_dp(ttj_job_costs: dict, yannakakis_job_costs: dict):
        result = []
        for pattern in ttj_conf[PATTERNS]:
            ttj_agg_dict = ttj_job_costs[pattern]
            yannakakis_agg_dict = yannakakis_job_costs[pattern]
            cost_diff = yannakakis_agg_dict[yannakakis_conf[COST]] - ttj_agg_dict[ttj_conf[COST]]
            runtime_diff = yannakakis_agg_dict['runtime (ms)'] - ttj_agg_dict['runtime (ms)']
            if cost_diff > 0:
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
    for i in range(len(x)):
        ax.text(x[i], y[i], labels[i])
    plt.scatter(x, y)
    if enable_log_scale:
        filename = construct_fig_name(ttj_conf, f"true_cost_diff_{ttj_conf[FILENAME_SIGNATURE]}_log.pdf")
    else:
        filename = construct_fig_name(ttj_conf, f"true_cost_diff_{ttj_conf[FILENAME_SIGNATURE]}.pdf")
    plt.savefig(filename, format='pdf')
    plt.show()


class Data(object):
    def __init__(self):
        self.num_dp_in_quads_by_ratio = [0, 0, 0, 0]

    def generate(self) -> str:
        return f"""
        num_dp_in_quads_by_ratio: {self.num_dp_in_quads_by_ratio}
        """


class Report(object):
    def __init__(self, ttj_conf: dict, yannakakis_conf: dict):
        self.ttj_conf = ttj_conf
        self.yannakakis_conf = yannakakis_conf
        self.data = Data()
        self.__perform_basic_data_analysis()

    def generate(self):
        print(self.data.generate())

    def __perform_basic_data_analysis(self):
        self.__generate_num_dp_in_quads_by_ratio()

    def __generate_num_dp_in_quads_by_ratio(self):
        @precondition(lambda ttj_job_costs, yannakakis_job_costs: len(ttj_job_costs) == len(yannakakis_job_costs),
                      "input costs have different length")
        def compute_ratio(ttj_job_costs: dict, yannakakis_job_costs: dict):
            result = []
            for pattern in self.ttj_conf[PATTERNS]:
                ttj_agg_dict = ttj_job_costs[pattern]
                yannakakis_agg_dict = yannakakis_job_costs[pattern]
                cost_ratio = yannakakis_agg_dict[self.yannakakis_conf[COST]] / ttj_agg_dict[self.ttj_conf[COST]]
                runtime_ratio = yannakakis_agg_dict['runtime (ms)'] / ttj_agg_dict['runtime (ms)']
                result.append({"query": pattern, "cost_ratio": cost_ratio, "runtime_ratio": runtime_ratio})
            return result

        ttj_job_costs = gather_data(self.ttj_conf)
        yannakakis_job_costs = gather_data(self.yannakakis_conf)
        result = compute_ratio(ttj_job_costs, yannakakis_job_costs)
        for result_dict in result:
            cost_ratio = result_dict['cost_ratio']
            runtime_ratio = result_dict['runtime_ratio']
            if cost_ratio > 1 and runtime_ratio > 1:
                self.data.num_dp_in_quads_by_ratio[0] += 1
            elif cost_ratio < 1 and runtime_ratio > 1:
                self.data.num_dp_in_quads_by_ratio[1] += 1
            elif cost_ratio < 1 and runtime_ratio < 1:
                self.data.num_dp_in_quads_by_ratio[2] += 1
            elif cost_ratio > 1 and runtime_ratio < 1:
                self.data.num_dp_in_quads_by_ratio[3] += 1


if __name__ == "__main__":
    # plot_dot_plots(TTJ_JOB_COST, Yannakakis_JOB_COST, False)
    # plot_dot_plots(TTJ_JOB_COST, Yannakakis_JOB_COST, True)
    # Report(TTJ_JOB_COST, Yannakakis_JOB_COST).generate()
    # plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_COST, Yannakakis_JOB_COST, False)
    # plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_COST, Yannakakis_JOB_COST, True)
    #
    # plot_dot_plots(TTJ_BACKJUMP_COST, Yannakakis_BACKJUMP_COST, False)
    # plot_dot_plots(TTJ_BACKJUMP_COST, Yannakakis_BACKJUMP_COST, True)
    # plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_BACKJUMP_COST, Yannakakis_BACKJUMP_COST, False)
    # plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_BACKJUMP_COST, Yannakakis_BACKJUMP_COST, True)

    plot_dot_plots(TTJ_JOB_SIMPLE_COST, Yannakakis_JOB_SIMPLE_COST, False, False)
    # plot_dot_plots(TTJ_JOB_SIMPLE_COST, Yannakakis_JOB_SIMPLE_COST, True)
    # Report(TTJ_JOB_SIMPLE_COST, Yannakakis_JOB_SIMPLE_COST).generate()
    # plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_SIMPLE_COST, Yannakakis_JOB_SIMPLE_COST, False)
    # plot_dots_that_ttj_cost_smaller_than_y_cost(TTJ_JOB_SIMPLE_COST, Yannakakis_JOB_SIMPLE_COST, True)
