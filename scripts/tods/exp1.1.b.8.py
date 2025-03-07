"""
Scatter plot of JOB results
"""
import logging
from pathlib import Path

from matplotlib import pyplot as plt

from icde2025.postgres_perf import process_job_enforced_raw_data
from plot.constants import DATA_SOURCE_CSV, HJ, COLUMN_RIGHT_BOUND, TTJ_COLOR, \
    JOB_SQLITE_ORDERING_RESULTS_INTROW_ON, POSTGRES, TTJ, Yannakakis1Pass, TTJ_VANILLA
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument

logging.getLogger('matplotlib.font_manager').disabled = True


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


def plot_job2(algorithm_pair):
    """
    So Fig 5 becomes 3 plots: HJ v.s. Postgres, TTJ v.s. HJ, TTJ v.s. YA
    """
    job_plot = {
        DATA_SOURCE_CSV: JOB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])
    prem_data[POSTGRES] = process_job_enforced_raw_data()

    data = dict()
    data[algorithm_pair.x_axis_algorithm] = prem_data[algorithm_pair.x_axis_algorithm]
    data[algorithm_pair.y_axis_algorithm] = prem_data[algorithm_pair.y_axis_algorithm]

    for dps in data.values():
        check_argument(len(dps) == job_plot[COLUMN_RIGHT_BOUND] - 1,
                       f"some query data is missing. There should be 113 dps. Instead, we have {len(dps)}")

    query_labels = list(prem_data[HJ].keys())
    x_axis_algorithm_time, y_axis_algorithm_time = [], []

    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == algorithm_pair.x_axis_algorithm:
                x_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == algorithm_pair.y_axis_algorithm:
                y_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)

    # Create a scatter plot
    f, ax = plt.subplots(figsize=(6, 6))
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    ax.set_yscale('log')
    ax.set_xscale('log')
    plt.xlim(0.1, 30)
    plt.ylim(0.1, 30)
    ax.set_aspect('equal', adjustable='box')

    axis_label_font_size = 20

    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        label = r'$\mathsf{HJ}$'
        plt.xlabel(r"$\mathsf{PostgreSQL}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        label = r'$\mathsf{TTJ}^{dp+ng}$'
        plt.xlabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}^{dp+ng}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)
    elif algorithm_pair.x_axis_algorithm == Yannakakis1Pass and algorithm_pair.y_axis_algorithm == TTJ:
        label = r'$\mathsf{TTJ}^{dp+ng}$'
        plt.xlabel(r"$\mathsf{YA}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}^{dp+ng}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ_VANILLA:
        label = r'$\mathsf{TTJ}$'
        plt.xlabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)

    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.legend(loc='best', frameon=True, ncol=2, prop=font2)

    plt.tight_layout()
    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        plt.savefig("job-hj-postgres.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("job-ttj-opt-hj.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == Yannakakis1Pass and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("job-ttj-opt-ya.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ_VANILLA:
        plt.savefig("job-ttj-vanilla-hj.pdf", format='pdf')


class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm


if __name__ == "__main__":
    plot_job2(AlgorithmPair(HJ, POSTGRES))
    plot_job2(AlgorithmPair(TTJ, HJ))
    plot_job2(AlgorithmPair(TTJ, Yannakakis1Pass))
    plot_job2(AlgorithmPair(TTJ_VANILLA, HJ))
