"""
Plot scatter plot of TTJHP under TTJ ordering vs. TTJHP under SQLite ordering
"""
import logging
from pathlib import Path

from matplotlib import pyplot as plt

from plot.constants import JOB_SQLITE_ORDERING_RESULTS_INTROW_ON, DATA_SOURCE_CSV, COLUMN_RIGHT_BOUND, \
    JOB_TTJ_ORDERING_RESULTS_INTROW_ON, HJ, TTJ, JOB_COLOR
from plot.cost_model4 import extract_data_from_csv

logging.getLogger('matplotlib.font_manager').disabled = True


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


def get_job_ttj_ordering_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / csv_name


class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm


def plot(algorithm_pair):
    job_plot = {
        DATA_SOURCE_CSV: JOB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        COLUMN_RIGHT_BOUND: 114
    }
    job_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                     column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])

    job_plot_ttj_ordering = {
        DATA_SOURCE_CSV: JOB_TTJ_ORDERING_RESULTS_INTROW_ON,
        COLUMN_RIGHT_BOUND: 114
    }
    job_data_ttj_ordering = extract_data_from_csv(
        get_job_ttj_ordering_full_path(job_plot_ttj_ordering[DATA_SOURCE_CSV]),
        column_range=[1, job_plot_ttj_ordering[COLUMN_RIGHT_BOUND]])

    job_x_axis_algorithm_time, job_y_axis_algorithm_time = [], []

    # Add JOB data
    query_labels = list(job_data[HJ].keys())
    for query_label in query_labels:
        for algorithm in job_data:
            if algorithm == algorithm_pair.x_axis_algorithm:
                job_x_axis_algorithm_time.append(job_data[algorithm][query_label] / 1000)

    query_labels = list(job_data[HJ].keys())
    for query_label in query_labels:
        for algorithm in job_data_ttj_ordering:
            if algorithm == algorithm_pair.y_axis_algorithm:
                job_y_axis_algorithm_time.append(job_data_ttj_ordering[algorithm][query_label] / 1000)

    # Create a scatter plot
    f, ax = plt.subplots(figsize=(6, 6))
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_aspect('equal', adjustable='box')

    axis_label_font_size = 20

    if algorithm_pair.x_axis_algorithm == TTJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.01, 300)
        plt.ylim(0.01, 300)
        plt.xlabel(r"$\mathsf{TTJ}^{OPT}$ (SQLite) time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}^{OPT}$ (Own) time (s)", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")

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
    if algorithm_pair.x_axis_algorithm == TTJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("ttj-opt-own-ttj-opt-sqlite-order.pdf", format='pdf')


if __name__ == '__main__':
    plot(AlgorithmPair(TTJ, TTJ))
