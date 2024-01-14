"""
Plot runtime ratio vs. intermediate results ratio on TTJ vs. HJ
"""
import csv

import matplotlib
from adjustText import adjust_text
from matplotlib import pyplot as plt
from seaborn import regplot

from plot.constants import FIG_SAVE_LOCATION
from plot.job import construct_fig_name
from plot.utility import render_filename, FILENAME_SIGNATURE
from precondition import precondition



def gather():
    hash_join_aggregate_csv = "HASH_JOIN_JOB_true_costMeasurements.csv"
    ttj_aggregate_csv = "TTJHP_JOB_true_costMeasurements.csv"

    hash_join_intermediate = dict()
    ttj_intermediate = dict()
    for agg_csv in [hash_join_aggregate_csv, ttj_aggregate_csv]:
        with open(agg_csv) as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    query_names = row
                    line_count += 1
                elif row[0] == 'cost':
                    for i in range(1, len(query_names)):
                        if 'HASH_JOIN' in agg_csv:
                            hash_join_intermediate[query_names[i]] = float(row[i])
                        elif 'TTJHP' in agg_csv:
                            ttj_intermediate[query_names[i]] = float(row[i])

    perf_report_csv = "2023-08-03T00:01:33.555478Z_perf_report.csv"
    hash_join_runtime = dict()
    ttj_runtime = dict()
    with open(perf_report_csv) as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                line_count += 1
                target_idx = dict()
                for i in range(1, len(row)):
                    if row[i] in query_names:
                        target_idx[i] = row[i]
            elif row[0] == 'HASH_JOIN':
                for i in target_idx.keys():
                    hash_join_runtime[target_idx[i]] = float(row[i])
            elif row[0] == 'TTJHP':
                for i in target_idx.keys():
                    ttj_runtime[target_idx[i]] = float(row[i])

    return hash_join_runtime, ttj_runtime, hash_join_intermediate, ttj_intermediate


def plot():
    """
    We use the actual join output size instead of the cross product upper bound to compute the total cost and
    then compute the ratio.
    """
    def compute_ratio(hash_join_runtime, ttj_runtime, hash_join_intermediate, ttj_intermediate):
        result = []
        for query in hash_join_runtime:
            intermediate_result_ratio = hash_join_intermediate[query] - ttj_intermediate[query]
            runtime_ratio = hash_join_runtime[query] - ttj_runtime[query]
            print(f"intermediate_result_diff: ({intermediate_result_ratio}) & runtime_diff: ({runtime_ratio}): {query}")
            # if intermediate_result_ratio > 1 and runtime_ratio > 1:
            #     print(f"intermediate_result_ratio > 1 ({intermediate_result_ratio}) & runtime_ratio > 1 ({runtime_ratio}): {query}")
            # if intermediate_result_ratio > 1 and runtime_ratio < 1:
            #     print(f"intermediate_result_ratio > 1 & runtime_ratio < 1: {query}")
            result.append({"query": query, "cost_ratio": intermediate_result_ratio, "runtime_ratio": runtime_ratio})
        return result

    hash_join_runtime, ttj_runtime, hash_join_intermediate, ttj_intermediate = gather()
    result = compute_ratio(hash_join_runtime, ttj_runtime, hash_join_intermediate, ttj_intermediate)
    x = [d['cost_ratio'] for d in result]
    y = [d['runtime_ratio'] for d in result]
    labels = [d['query'] for d in result]
    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(4, 4), dpi=140)
    ax.minorticks_off()
    plt.grid(which='major', axis='both', zorder=-1.0)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    # plt.ylabel('HJ runtime - TTJ runtime', fontdict=font2)
    plt.ylabel('Execution time difference', fontdict=font2)
    # plt.xlabel('(HJ inter. result size + sum of base tables) - (TTJ inter. result size + sum of base tables in clean state)', fontdict=font2)
    plt.xlabel('Logical cost difference', fontdict=font2)
    plt.axhline(y=1, color='#FF0000', linestyle='-')
    plt.axvline(x=1, color='#FF0000', linestyle='-')
    texts = []
    # Comments out this for loop for "*false_costMeasurements.csv"
    # for i in range(len(x)):
    #     if x[i] > 0 and y[i] < 0:
    #         label = labels[i] if labels[i] != 'q8c' else 'q8'
    #         texts.append(plt.text(x[i], y[i], label))
    # Remove ci=None if we want to see confidence interval (region) around line: https://stackoverflow.com/a/61524079/1460102
    regplot(x=x, y=y, fit_reg=True, scatter=True, ci=None)
    # adjust_text(texts, expand_points=(2, 2),
    #             arrowprops=dict(arrowstyle="-", color='k', lw=0.5))
    ttj_conf = {
        FIG_SAVE_LOCATION: r"../../results/others/simple-cost-model-with-predicates/img/",
        FILENAME_SIGNATURE: "job"
    }
    filename = construct_fig_name(ttj_conf,
                                  render_filename(f"runtime_intermediate_ratio_{ttj_conf[FILENAME_SIGNATURE]}"))
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()

if __name__ == "__main__":
    plot()