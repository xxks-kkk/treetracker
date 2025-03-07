"""
Scatter plot of JOB results
"""
import logging
from pathlib import Path

from matplotlib import pyplot as plt

from icde2025.postgres_perf import process_job_raw_data
from plot.constants import DATA_SOURCE_CSV, HJ, COLUMN_RIGHT_BOUND, TTJ_COLOR, \
    JOB_SQLITE_ORDERING_RESULTS_INTROW_ON, POSTGRES, TTJ, TTJ_LINEAR, JOB_POSTGRES_ORDERGING_RESULTS_INTROW_OFF
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument

logging.getLogger('matplotlib.font_manager').disabled = True


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "perf_on_postgres_plans" / csv_name


def get_job_full_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


def plot_job2(algorithm_pair):
    """
    Fig 8 becomes: HJ v.s. Postgres, TTJ v.s. HJ, TTJ v.s. TTJ^L
    """
    job_plot_linear_plan = {
        DATA_SOURCE_CSV: JOB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        COLUMN_RIGHT_BOUND: 114
    }

    job_plot = {
        DATA_SOURCE_CSV: JOB_POSTGRES_ORDERGING_RESULTS_INTROW_OFF,
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])
    prem_data[POSTGRES] = process_job_raw_data()
    prem_data_linear = extract_data_from_csv(get_job_full_path_linear(job_plot_linear_plan[DATA_SOURCE_CSV]),
                                             column_range=[1, job_plot_linear_plan[COLUMN_RIGHT_BOUND]])

    query_labels = list(prem_data[HJ].keys())
    x_axis_algorithm_time, y_axis_algorithm_time = [], []

    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == algorithm_pair.x_axis_algorithm:
                x_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == algorithm_pair.y_axis_algorithm:
                y_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
    if algorithm_pair.x_axis_algorithm == TTJ_LINEAR:
        check_argument(len(x_axis_algorithm_time) == 0, 'x_axis_algorithm_time should be empty')
        for query_label in query_labels:
            for algorithm in prem_data_linear:
                if algorithm == TTJ:
                    x_axis_algorithm_time.append(prem_data_linear[algorithm][query_label] / 1000)

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
        label = r'$\mathsf{TTJ}$'
        plt.xlabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)
    elif algorithm_pair.x_axis_algorithm == TTJ_LINEAR and algorithm_pair.y_axis_algorithm == TTJ:
        label = r'$\mathsf{TTJ}$'
        plt.xlabel(r'$\mathsf{TTJ}^{L}$ time (s)', fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)

    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.legend(loc='best', frameon=True, ncol=2, prop=font2)

    plt.tight_layout()
    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        plt.savefig("bushy-hj-postgres.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("bushy-ttj-hj.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == TTJ_LINEAR and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("bushy-ttj-ttj-l.pdf", format='pdf')


class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm


if __name__ == "__main__":
    plot_job2(AlgorithmPair(HJ, POSTGRES))
    plot_job2(AlgorithmPair(TTJ, HJ))
    plot_job2(AlgorithmPair(TTJ, TTJ_LINEAR))
