"""
Scatter plot of SSB
"""

from icde2025.postgres_perf import process_ssb_enforced_raw_data

"""
Scatter plot of JOB results
"""
import logging
from pathlib import Path

from matplotlib import pyplot as plt

from plot.constants import DATA_SOURCE_CSV, HJ, COLUMN_RIGHT_BOUND, TTJ_COLOR, \
    POSTGRES, TTJ, Yannakakis1Pass, TTJ_VANILLA, SSB_SQLITE_ORDERING_RESULTS_INTROW_ON
from plot.job import extract_data_from_csv

logging.getLogger('matplotlib.font_manager').disabled = True


def get_ssb_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / "hj_ordering_hj" / csv_name


def plot_ssb(algorithm_pair):
    """
    So Fig 5 becomes 3 plots: HJ v.s. Postgres, TTJ v.s. HJ, TTJ v.s. YA

    Since the queries where TTJ is most visibly slower than HJ are mostly due to no good overhead,
    can you also try disabling the optimizations and plot TTJ against HJ for all three benchmarks?
    """
    ssb_plot_with_predicates = {
        # SF=1, IntRow Optimization enabled
        DATA_SOURCE_CSV: SSB_SQLITE_ORDERING_RESULTS_INTROW_ON,
        # SF=1, IntRow Optimization disabled
        # DATA_SOURCE_CSV: "benchmarkssb-result-2024-07-15t23:56:26.1061benchmarkssb-result-2024-12-09t17:27:00.748485benchmarkssb-result-2024-12-11t01:07:14.181061_perf_report.csv",
        COLUMN_RIGHT_BOUND: 14,
    }
    prem_data, _ = extract_data_from_csv(get_ssb_full_path(ssb_plot_with_predicates[DATA_SOURCE_CSV]),
                                         column_range=[1, ssb_plot_with_predicates[COLUMN_RIGHT_BOUND]])
    prem_data[POSTGRES] = process_ssb_enforced_raw_data()
    labels = ["1.1", "1.2", "1.3", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3"]

    x_axis_algorithm_time = [speed / 1000 for speed in prem_data[algorithm_pair.x_axis_algorithm]]
    y_axis_algorithm_time = [speed / 1000 for speed in prem_data[algorithm_pair.y_axis_algorithm]]

    # query_labels = list(prem_data[HJ].keys())
    # x_axis_algorithm_time, y_axis_algorithm_time = [], []
    #
    # for query_label in query_labels:
    #     for algorithm in prem_data:
    #         if algorithm == algorithm_pair.x_axis_algorithm:
    #             x_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)
    #         elif algorithm == algorithm_pair.y_axis_algorithm:
    #             y_axis_algorithm_time.append(prem_data[algorithm][query_label] / 1000)

    # Create a scatter plot
    f, ax = plt.subplots(figsize=(6, 6))
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    axis_label_font_size = 20

    if algorithm_pair.x_axis_algorithm == POSTGRES and algorithm_pair.y_axis_algorithm == HJ:
        ax.set_yscale('log')
        ax.set_xscale('log')
        plt.xlim(0.01, 2)
        plt.ylim(0.01, 2)
        ax.set_aspect('equal', adjustable='box')
        label = r'$\mathsf{HJ}$'
        plt.xlabel(r"$\mathsf{PostgreSQL}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        ax.set_yscale('log')
        ax.set_xscale('log')
        plt.xlim(0.01, 2)
        plt.ylim(0.01, 2)
        ax.set_aspect('equal', adjustable='box')
        label = r'$\mathsf{TTJ}^{dp+ng}$'
        plt.xlabel(r"$\mathsf{HJ}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}^{dp+ng}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)
    elif algorithm_pair.x_axis_algorithm == Yannakakis1Pass and algorithm_pair.y_axis_algorithm == TTJ:
        ax.set_yscale('log')
        ax.set_xscale('log')
        plt.xlim(0.01, 10)
        plt.ylim(0.01, 10)
        ax.set_aspect('equal', adjustable='box')
        # DEBUG to make sure we have all the points plotted
        # texts = []
        # for i, label in enumerate(labels):
        #     texts.append(plt.annotate(label,
        #                               (x_axis_algorithm_time[i], y_axis_algorithm_time[i]), xytext=(10, 0),
        #                               textcoords='offset points',
        #                               arrowprops=dict(arrowstyle="->", connectionstyle="arc3,rad=.2"),
        #                               ha='left', va='center'))
        # adjust_text(texts, autoalign='y',  # Adjust text positions
        #             arrowprops=dict(arrowstyle="-", color='black', lw=0.5),  # Adjust arrow style
        #             expand_points=(2, 2),  # Expand the area around points to avoid collisions
        #             only_move={'points': 'y', 'text': 'y'})  # Only move points and texts in y-direction
        label = r'$\mathsf{TTJ}^{dp+ng}$'
        plt.xlabel(r"$\mathsf{YA}$ time (s)", fontsize=axis_label_font_size)
        plt.ylabel(r"$\mathsf{TTJ}^{dp+ng}$ time (s)", fontsize=axis_label_font_size)
        plt.scatter(x_axis_algorithm_time, y_axis_algorithm_time, s=20, color=TTJ_COLOR, label=label)
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ_VANILLA:
        ax.set_yscale('log')
        ax.set_xscale('log')
        plt.xlim(0.01, 2)
        plt.ylim(0.01, 2)
        ax.set_aspect('equal', adjustable='box')
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
        plt.savefig("ssb-hj-postgres.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("ssb-ttj-opt-hj.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == Yannakakis1Pass and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("ssb-ttj-opt-ya.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == HJ and algorithm_pair.y_axis_algorithm == TTJ_VANILLA:
        plt.savefig("ssb-ttj-vanilla-hj.pdf", format='pdf')


class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm


if __name__ == "__main__":
    plot_ssb(AlgorithmPair(HJ, POSTGRES))
    plot_ssb(AlgorithmPair(TTJ, HJ))
    plot_ssb(AlgorithmPair(TTJ, Yannakakis1Pass))
    plot_ssb(AlgorithmPair(TTJ_VANILLA, HJ))
