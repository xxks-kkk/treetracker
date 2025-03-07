"""
Present ablation study results in Free Join style. That is,

Can you do a scatter plot of the absolute time like the free join paper? That might make things easier to see.
That is, do a scatter plot each for $\mathsf{TTJ}^{dp+ng}$ vs $\mathsf{TTJ}$,
$\mathsf{TTJ}^{dp}$ vs $\mathsf{TTJ}$, and $\mathsf{TTJ}^{ng}$ vs $\mathsf{TTJ}$.
"""

"""
Experiment 1.1: Distribution of individual query evaluation time

multiple bar graphs + broken axis + label at bottom

Compared to exp1.1.b.4, this adds Postgres execution time.
Compared to exp1.1.b.5, Postgres execution time is based on Postgres using SQLite order on a left-deep plan.
"""
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt

from plot.constants import DATA_SOURCE_CSV, ALGORITHMS_TO_PLOT, HJ, TTJ, COLUMN_RIGHT_BOUND, TTJ_COLOR, \
    TTJ_NO_NG, TTJ_VANILLA, TTJ_NO_DP, JOB_SQLITE_ORDERING_RESULTS_INTROW_ON
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


def plot_job_ad_hoc():
    """
    Plot TTJ^{dp+ng} vs. TTJ^{dp} performance on JOB in Wang23 style
    """
    job_plot = {
        DATA_SOURCE_CSV: "2024-11-24T23:40:33.613481Z_perf_report.csv",
        ALGORITHMS_TO_PLOT: [TTJ, TTJ_NO_DP],
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])
    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in job_plot[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    for dps in data.values():
        check_argument(len(dps) == job_plot[COLUMN_RIGHT_BOUND] - 1,
                       f"some query data is missing. There should be 113 dps. Instead, we have {len(dps)}")

    query_labels = list(prem_data[HJ].keys())
    ttj_no_dp_time, ttj_time = [], []
    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == TTJ_NO_DP:
                ttj_no_dp_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == TTJ:
                ttj_time.append(prem_data[algorithm][query_label] / 1000)
    ttj_no_dp_time = np.array(ttj_no_dp_time)
    ttj_time = np.array(ttj_time)

    # Create a scatter plot
    # plt.scatter(hash_join_time, hash_join_time)
    f, ax = plt.subplots(figsize=(6, 6))
    # ax.axline((1, 1), slope=1, color='red', label=r"$\mathsf{HJ}$")
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    # plt.scatter(hash_join_time, hash_join_time, s=5, label=r'$\mathsf{HJ}$')
    ax.set_yscale('log')
    ax.set_xscale('log')
    plt.xlim(1, 100)
    plt.ylim(1, 100)
    ax.set_aspect('equal', adjustable='box')

    plt.scatter(ttj_no_dp_time, ttj_time, s=5, color=TTJ_COLOR, label=r'$\mathsf{TTJ}^{dp+ng}$')

    # Set the axis labels
    plt.xlabel(r"$\mathsf{TTJ}^{ng}$ time (s)")
    plt.ylabel(r"$\mathsf{TTJ}^{dp+ng}$ time (s)")

    # Add a grid
    # plt.grid(True)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    plt.legend(loc='best', frameon=True, ncol=2, prop=font2)

    plt.tight_layout()
    plt.savefig("ttj-dp-ng-ttj-ng.pdf", format='pdf')
    # Show the plot
    plt.show()


def ttj_perf_analysis(algorithm_to_plot, algorithm_to_plot_time, ttj_vanilla_time, query_labels):
    count_ttj_fastest = 0
    total_queries = len(algorithm_to_plot_time)
    maximum_diff = 0
    maximum_diff_achieve_at = 0
    accumulate_diff = 0
    rank = []

    for idx in range(len(algorithm_to_plot_time)):
        if algorithm_to_plot_time[idx] < ttj_vanilla_time[idx]:
            count_ttj_fastest += 1
            accumulate_diff += (ttj_vanilla_time[idx] - algorithm_to_plot_time[idx])
            rank.append(ttj_vanilla_time[idx] - algorithm_to_plot_time[idx])
            if (ttj_vanilla_time[idx] - algorithm_to_plot_time[idx]) > maximum_diff:
                maximum_diff = ttj_vanilla_time[idx] - algorithm_to_plot_time[idx]
                maximum_diff_achieve_at = idx
    rank = sorted(rank, reverse=True)

    print(
        f"total queries: {total_queries}, number of queries that {algorithm_to_plot} is the fastest: {count_ttj_fastest}")
    print(f"maximum diff: {maximum_diff} for query: {query_labels[maximum_diff_achieve_at]}")
    print(f"average diff: {accumulate_diff / count_ttj_fastest}")
    print(f"rank: {rank}")


def plot_job2(algorithm_to_plot):
    """
    Plot algorithm_to_plot, e.g., TTJ^{dp+ng} vs. TTJ performance on JOB in Wang23 style
    """
    job_plot = {
        DATA_SOURCE_CSV: JOB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        ALGORITHMS_TO_PLOT: [algorithm_to_plot, TTJ_VANILLA],
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])
    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in job_plot[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    for dps in data.values():
        check_argument(len(dps) == job_plot[COLUMN_RIGHT_BOUND] - 1,
                       f"some query data is missing. There should be 113 dps. Instead, we have {len(dps)}")

    query_labels = list(prem_data[HJ].keys())
    ttj_vanilla_time, algorithm_to_plot_time, hj_time = [], [], []
    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == TTJ_VANILLA:
                ttj_vanilla_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == algorithm_to_plot:
                algorithm_to_plot_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == HJ:
                hj_time.append(prem_data[algorithm][query_label] / 1000)
    ttj_vanilla_time = np.array(ttj_vanilla_time)
    algorithm_to_plot_time = np.array(algorithm_to_plot_time)
    hj_time = np.array(hj_time)

    print(f"analysis against {TTJ_VANILLA}")
    ttj_perf_analysis(algorithm_to_plot, algorithm_to_plot_time, ttj_vanilla_time, query_labels)
    # print(f"analysis against {HJ}")
    # ttj_perf_analysis(algorithm_to_plot, algorithm_to_plot_time, hj_time, query_labels)

    # Create a scatter plot
    # plt.scatter(hash_join_time, hash_join_time)
    f, ax = plt.subplots(figsize=(6, 6))
    # ax.axline((1, 1), slope=1, color='red', label=r"$\mathsf{HJ}$")
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    # plt.scatter(hash_join_time, hash_join_time, s=5, label=r'$\mathsf{HJ}$')
    ax.set_yscale('log')
    ax.set_xscale('log')
    plt.xlim(1, 100)
    plt.ylim(1, 100)
    ax.set_aspect('equal', adjustable='box')

    if algorithm_to_plot == TTJ:
        label = r'$\mathsf{TTJ}^{dp+ng}$'
    elif algorithm_to_plot == TTJ_NO_NG:
        label = r'$\mathsf{TTJ}^{dp}$'
    elif algorithm_to_plot == TTJ_NO_DP:
        label = r'$\mathsf{TTJ}^{ng}$'

    # plt.scatter(hj_time, algorithm_to_plot_time, s=5, color=TTJ_COLOR, label = label)
    plt.scatter(ttj_vanilla_time, algorithm_to_plot_time, s=20, color=TTJ_COLOR, label=label)

    axis_label_font_size = 20
    # Set the axis labels
    plt.xlabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
    # plt.xlabel(r"$\mathsf{HJ}$ time (s)")
    if algorithm_to_plot == TTJ:
        plt.ylabel(r"$\mathsf{TTJ}^{dp+ng}$ time (s)", fontsize=axis_label_font_size)
    elif algorithm_to_plot == TTJ_NO_NG:
        plt.ylabel(r"$\mathsf{TTJ}^{dp}$ time (s)", fontsize=axis_label_font_size)
    elif algorithm_to_plot == TTJ_NO_DP:
        plt.ylabel(r"$\mathsf{TTJ}^{ng}$ time (s)", fontsize=axis_label_font_size)

    # Add a grid
    # plt.grid(True)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.legend(loc='best', frameon=True, ncol=2, prop=font2)

    plt.tight_layout()
    if algorithm_to_plot == TTJ:
        plt.savefig("ttj-dp-ng-ttj.pdf", format='pdf')
    elif algorithm_to_plot == TTJ_NO_NG:
        plt.savefig("ttj-dp-ttj.pdf", format='pdf')
    elif algorithm_to_plot == TTJ_NO_DP:
        plt.savefig("ttj-ng-ttj.pdf", format='pdf')
    # Show the plot
    # plt.show()


if __name__ == "__main__":
    plot_job2(TTJ)
    plot_job2(TTJ_NO_DP)
    plot_job2(TTJ_NO_NG)
    # plot_job_ad_hoc()
