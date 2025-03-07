"""
Scatter plot of all three benchmarks on native Postgres plans

Fig. 8 becomes 3 plots: HJ v.s. Postgres, TTJ v.s. HJ, TTJ v.s. TTJ^L
"""
import logging
from pathlib import Path

from matplotlib import pyplot as plt

from icde2025.postgres_perf import process_job_raw_data, process_ssb_raw_data, process_tpch_raw_data
from plot.constants import DATA_SOURCE_CSV, COLUMN_RIGHT_BOUND, JOB_SQLITE_ORDERING_RESULTS_INTROW_ON, \
    JOB_POSTGRES_ORDERGING_RESULTS_INTROW_OFF, POSTGRES, HJ, TTJ_LINEAR, TTJ, SSB_SQLITE_ORDERING_RESULTS_INTROW_ON, \
    SSB_POSTGRES_ORDERING_RESULTS_INTROW_OFF, JOB_COLOR, SSB_COLOR, \
    TPCH_SQLITE_ORDERING_RESULTS_INTROW_ON, TPCH_POSTGRES_ORDERING_RESULTS_INTROW_OFF, TPC_COLOR
from plot.cost_model4 import extract_data_from_csv
from plot.job import extract_data_from_csv as extract_data_from_csv2
from plot.utility import check_argument

logging.getLogger('matplotlib.font_manager').disabled = True


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "perf_on_postgres_plans" / csv_name


def get_job_full_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


def get_ssb_full_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / "hj_ordering_hj" / csv_name


def get_ssb_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / "perf_on_postgres_plans" / csv_name


def get_tpch_full_path_linear(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "tpch" / "with_predicates" / "hj_ordering_hj" / csv_name


def get_tpch_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "tpch" / "with_predicates" / "perf_on_postgres_plans" / csv_name


class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm


def plot(algorithm_pair):
    """
    Fig. 8 becomes 3 plots: HJ v.s. Postgres, TTJ v.s. HJ, TTJ v.s. TTJ^L
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

    ssb_plot_linear_plan = {
        # SF=1, IntRow Optimization enabled
        DATA_SOURCE_CSV: SSB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        # SF=1, IntRow Optimization disabled
        # DATA_SOURCE_CSV: "benchmarkssb-result-2024-07-15t23:56:26.1061benchmarkssb-result-2024-12-09t17:27:00.748485benchmarkssb-result-2024-12-11t01:07:14.181061_perf_report.csv",
        COLUMN_RIGHT_BOUND: 14,
    }

    ssb_plot = {
        DATA_SOURCE_CSV: SSB_POSTGRES_ORDERING_RESULTS_INTROW_OFF,
        COLUMN_RIGHT_BOUND: 14,
    }

    ssb_prem_data, _ = extract_data_from_csv2(get_ssb_full_path(ssb_plot[DATA_SOURCE_CSV]),
                                              column_range=[1, ssb_plot[COLUMN_RIGHT_BOUND]])
    ssb_prem_data[POSTGRES] = process_ssb_raw_data()
    ssb_prem_data_linear, _ = extract_data_from_csv2(
        get_ssb_full_path_linear(ssb_plot_linear_plan[DATA_SOURCE_CSV]),
        column_range=[1, ssb_plot_linear_plan[COLUMN_RIGHT_BOUND]])

    tpch_plot_linear_plan = {
        DATA_SOURCE_CSV: TPCH_SQLITE_ORDERING_RESULTS_INTROW_ON,
        COLUMN_RIGHT_BOUND: 14,
    }
    tpch_data_linear, _ = extract_data_from_csv2(get_tpch_full_path_linear(tpch_plot_linear_plan[DATA_SOURCE_CSV]),
                                                 column_range=[1, tpch_plot_linear_plan[COLUMN_RIGHT_BOUND]])
    tpch_plot = {
        DATA_SOURCE_CSV: TPCH_POSTGRES_ORDERING_RESULTS_INTROW_OFF,
        COLUMN_RIGHT_BOUND: 14,
    }
    tpch_data, _ = extract_data_from_csv2(get_tpch_full_path(tpch_plot[DATA_SOURCE_CSV]),
                                          column_range=[1, tpch_plot[COLUMN_RIGHT_BOUND]])
    tpch_data[POSTGRES] = process_tpch_raw_data()

    # Add JOB data
    query_labels = list(prem_data[HJ].keys())
    job_x_axis_algorithm_time, job_y_axis_algorithm_time = [], []

    for query_label in query_labels:
        for algorithm in prem_data:
            if algorithm == algorithm_pair.x_axis_algorithm:
                job_x_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
            elif algorithm == algorithm_pair.y_axis_algorithm:
                job_y_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
    if algorithm_pair.x_axis_algorithm == TTJ_LINEAR:
        check_argument(len(job_x_axis_algorithm_time) == 0, 'x_axis_algorithm_time should be empty')
        for query_label in query_labels:
            for algorithm in prem_data_linear:
                if algorithm == TTJ:
                    job_x_axis_algorithm_time.append(prem_data_linear[algorithm][query_label] / 1000)

    # Add SSB data
    if algorithm_pair.x_axis_algorithm != TTJ_LINEAR:
        ssb_x_axis_algorithm_time = [speed / 1000 for speed in ssb_prem_data[algorithm_pair.x_axis_algorithm]]
    else:
        ssb_x_axis_algorithm_time = [speed / 1000 for speed in ssb_prem_data_linear[TTJ]]
    ssb_y_axis_algorithm_time = [speed / 1000 for speed in ssb_prem_data[algorithm_pair.y_axis_algorithm]]

    # Add TPC-H data
    if algorithm_pair.x_axis_algorithm != TTJ_LINEAR:
        tpch_x_axis_algorithm_time = [speed / 1000 for speed in tpch_data[algorithm_pair.x_axis_algorithm]]
    else:
        tpch_x_axis_algorithm_time = [speed / 1000 for speed in tpch_data_linear[TTJ]]
    tpch_y_axis_algorithm_time = [speed / 1000 for speed in tpch_data[algorithm_pair.y_axis_algorithm]]

    # Create a scatter plot
    f, ax = plt.subplots(figsize=(6, 6))
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_aspect('equal', adjustable='box')

    axis_label_font_size = 20

    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        plt.xlim(0.01, 300)
        plt.ylim(0.01, 300)
        plt.xlabel(r"$\mathsf{PostgreSQL}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR, label="TPC-H")
        plt.scatter(ssb_x_axis_algorithm_time, ssb_y_axis_algorithm_time, s=20, marker="x", color=SSB_COLOR,
                    label="SSB")
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.01, 300)
        plt.ylim(0.01, 300)
        plt.xlabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR, label="TPC-H")
        plt.scatter(ssb_x_axis_algorithm_time, ssb_y_axis_algorithm_time, s=20, marker="x", color=SSB_COLOR,
                    label="SSB")
    elif algorithm_pair.x_axis_algorithm == TTJ_LINEAR and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.01, 300)
        plt.ylim(0.01, 300)
        plt.xlabel(r'$\mathsf{TTJ}^{L}$ time (s)', fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(job_x_axis_algorithm_time, job_y_axis_algorithm_time, s=20, color=JOB_COLOR, label="JOB")
        plt.scatter(tpch_x_axis_algorithm_time, tpch_y_axis_algorithm_time, s=20, marker=",", color=TPC_COLOR, label="TPC-H")
        plt.scatter(ssb_x_axis_algorithm_time, ssb_y_axis_algorithm_time, s=20, marker="x", color=SSB_COLOR,
                    label="SSB")

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

    plt.tight_layout()
    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        plt.savefig("mega-bushy-hj-postgres.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("mega-bushy-ttj-hj.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == TTJ_LINEAR and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("mega-bushy-ttj-ttj-l.pdf", format='pdf')


if __name__ == "__main__":
    plot(AlgorithmPair(HJ, POSTGRES))
    plot(AlgorithmPair(TTJ, HJ))
    plot(AlgorithmPair(TTJ, TTJ_LINEAR))
