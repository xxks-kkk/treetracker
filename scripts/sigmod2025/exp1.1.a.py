"""
Experiment 1.1: Distribution of individual query evaluation time

Scatter plot with broken axis
"""
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from plot.constants import DATA_SOURCE_CSV, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, HJ, TTJ, Yannakakis, LIP
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument

def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / csv_name

def plot_job():
    """
    Plot HJ, TTJ, Yannakakis's algorithm performance on JOB in Wang23 style
    """
    job_plot = {
        DATA_SOURCE_CSV: "2023-11-15T07:13:39.823669Z_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis],
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
    hash_join_time, ttj_time, yannakakis_time = [],[],[]
    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == HJ:
                hash_join_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == TTJ:
                ttj_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == Yannakakis:
                yannakakis_time.append(prem_data[algorithm][query_label] / 1000)
    hash_join_time = np.array(hash_join_time)
    ttj_time = np.array(ttj_time)
    yannakakis_time = np.array(yannakakis_time)

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)
    fig.subplots_adjust(hspace=0.05)

    ax1.axline((1, 1), slope=1, color='red', label='HJ')
    ax1.scatter(hash_join_time, ttj_time, s=5, label = 'TTJ')
    ax1.scatter(hash_join_time, yannakakis_time, s=5, label = 'Yannakakis')

    ax2.axline((1, 1), slope=1, color='red', label='HJ')
    ax2.scatter(hash_join_time, ttj_time, s=5, label = 'TTJ')
    ax2.scatter(hash_join_time, yannakakis_time, s=5, label = 'Yannakakis')

    ax1.set_ylim(18, 70)
    ax2.set_ylim(0, 11)

    ax1.spines.bottom.set_visible(False)
    ax2.spines.top.set_visible(False)
    ax1.xaxis.tick_top()
    ax1.tick_params(labeltop=False)  # don't put tick labels at the top
    ax2.xaxis.tick_bottom()

    d = .5  # proportion of vertical to horizontal extent of the slanted line
    kwargs = dict(marker=[(-1, -d), (1, d)], markersize=12,
                  linestyle="none", color='k', mec='k', mew=1, clip_on=False)
    ax1.plot([0, 1], [0, 0], transform=ax1.transAxes, **kwargs)
    ax2.plot([0, 1], [1, 1], transform=ax2.transAxes, **kwargs)

    # Set the axis labels
    plt.xlabel("Hash Join (s)")
    plt.ylabel("TTJ / Yannakakis's algorithm (s)")

    # Add a grid
    # plt.grid(True)
    plt.legend(loc='best')

    plt.tight_layout()
    plt.savefig("exp1.1-scatter-broken-axis.pdf", format='pdf')
    # Show the plot
    plt.show()

if __name__ == "__main__":
    plot_job()