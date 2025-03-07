"""
Mimic Figure 14 in Free Join paper for the sake of TTJ presentation slides.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from plot.constants import DATA_SOURCE_CSV, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, HJ, TTJ, Yannakakis, LIP, \
    YannakakisB, TTJ_COLOR, Yannakakis1Pass, Yannakakis1Pass_COLOR
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument

def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name



def plot_job():
    """
    Plot HJ, TTJ, Yannakakis's algorithm performance on JOB in Wang23 style
    """
    job_plot = {
        DATA_SOURCE_CSV: "benchmarkjobwithpredicatesfixedhjorderingshallow-result-2024-07-07t17:19:27.127911benchmarkjobwithpredicatesfixedhjorderingshallow-result-2024-07-22t16:51:01.516062_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ],
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
    hash_join_time, ttj_time, yannakakis_time, yannakakisB_time = [],[],[],[]
    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == HJ:
                hash_join_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == TTJ:
                ttj_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == Yannakakis:
                yannakakis_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == YannakakisB:
                yannakakisB_time.append(prem_data[algorithm][query_label] / 1000)
    hash_join_time = np.array(hash_join_time)
    ttj_time = np.array(ttj_time)
    yannakakis_time = np.array(yannakakis_time)
    yannakakisB_time = np.array(yannakakisB_time)

    # Create a scatter plot
    # plt.scatter(hash_join_time, hash_join_time)
    f, ax = plt.subplots(figsize=(6, 6))
    # ax.axline((1, 1), slope=1, color='red', label=r"$\mathsf{HJ}$")
    ax.axline((1, 1), slope=1, color='#5b5b5b',linewidth=0.2)
    # plt.scatter(hash_join_time, hash_join_time, s=5, label=r'$\mathsf{HJ}$')
    ax.set_yscale('log')
    ax.set_xscale('log')

    plt.xlim(1, 100)
    plt.ylim(1, 100)
    ax.set_aspect('equal', adjustable='box')

    plt.scatter(hash_join_time, ttj_time, s=5, color=TTJ_COLOR, label = r'$\mathsf{TTJ}$')

    # Set the axis labels
    plt.xlabel(r"Binary join time (s)")
    plt.ylabel(r"$\mathsf{TTJ}$ time (s)")

    # Add a grid
    # plt.grid(True)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    plt.legend(loc='best', frameon=True, ncol=2, prop=font2)

    plt.tight_layout()
    plt.savefig("free-join-1.pdf", format='pdf')
    # Show the plot
    plt.show()

def plot_job2():
    """
    Plot HJ, TTJ, Yannakakis's algorithm performance on JOB in Wang23 style
    """
    job_plot = {
        DATA_SOURCE_CSV: "benchmarkjobwithpredicatesfixedhjorderingshallow-result-2024-07-07t17:19:27.127911benchmarkjobwithpredicatesfixedhjorderingshallow-result-2024-07-22t16:51:01.516062_perf_report.csv",
        ALGORITHMS_TO_PLOT: [Yannakakis1Pass, TTJ],
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
    hash_join_time, ttj_time, yannakakis1Pass_time = [],[],[]
    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == HJ:
                hash_join_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == TTJ:
                ttj_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == Yannakakis1Pass:
                yannakakis1Pass_time.append(prem_data[algorithm][query_label] / 1000)
    hash_join_time = np.array(hash_join_time)
    ttj_time = np.array(ttj_time)
    yannakakis1Pass_time = np.array(yannakakis1Pass_time)

    # Create a scatter plot
    # plt.scatter(hash_join_time, hash_join_time)
    f, ax = plt.subplots(figsize=(6, 6))
    # ax.axline((1, 1), slope=1, color='red', label=r"$\mathsf{HJ}$")
    ax.axline((1, 1), slope=1, color='#5b5b5b',linewidth=0.2)


    # plt.scatter(hash_join_time, hash_join_time, s=5, label=r'$\mathsf{HJ}$')
    ax.set_yscale('log')
    ax.set_xscale('log')
    plt.xlim(1, 100)
    plt.ylim(1, 100)
    ax.set_aspect('equal', adjustable='box')

    plt.scatter(yannakakis1Pass_time, ttj_time, s=5, color=TTJ_COLOR, label = r'$\mathsf{TTJ}$')

    # Set the axis labels
    plt.xlabel(r"$\mathsf{YA}$ time (s)")
    plt.ylabel(r"$\mathsf{TTJ}$ time (s)")

    # Add a grid
    # plt.grid(True)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    plt.legend(loc='best', frameon=True, ncol=2, prop=font2)

    plt.tight_layout()
    plt.savefig("free-join-2.pdf", format='pdf')
    # Show the plot
    plt.show()

if __name__ == "__main__":
    plot_job()
    plot_job2()