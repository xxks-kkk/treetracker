"""
Experiment 1.1: Distribution of individual query evaluation time

TPC-H bar graph (including SQLite performance)
"""
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt, transforms, ticker
from scipy.stats import gmean

from icde2025.sqlite_perf import process_tpch_raw_data
from plot.constants import DATA_SOURCE_CSV, DECIMAL_PRECISION, HJ, PLOT_FUNCTION, FIG_SAVE_LOCATION, \
    SORT_DESCENDING_BASED_ON_HJ, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, TTJ, Yannakakis, YannakakisB, YannakakisV, \
    Yannakakis1Pass, TTJ_COLOR, Yannakakis1Pass_COLOR, SQLITE, SQLITE_COLOR, HJ_COLOR
from plot.job import extract_data_from_csv, construct_fig_name
from plot.utility import check_argument, convert_time, TimeUnits

def get_tpch_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "tpch" / "with_predicates" / "hj_ordering_hj" / csv_name

def speedup_analysis(data_speedup, labels):
    """
    Compute max, min, geometric mean of speedups for each algorithm
    """
    for algorithm in data_speedup:
        speed_vals = np.array(data_speedup[algorithm])
        min_idx = np.argmin(speed_vals)
        min_val = np.min(speed_vals)
        max_idx = np.argmax(speed_vals)
        max_val = np.max(speed_vals)
        geo_mean = gmean(speed_vals)
        print(f"""
        algorithm : {algorithm}
        min       : {min_val}
        min_query : {labels[min_idx]}
        max       : {max_val}
        max_query : {labels[max_idx]}
        geo_mean  : {geo_mean}
        """)


def ttj_perf_analysis(grouped_data, labels):
    count_ttj_fastest = 0
    total_queries = len(grouped_data[TTJ])

    for idx in range(len(grouped_data[TTJ])):
        if grouped_data[TTJ][idx] < grouped_data[HJ][idx] and \
            grouped_data[TTJ][idx] < grouped_data[Yannakakis1Pass][idx]:
            count_ttj_fastest += 1

    print(f"total queries: {total_queries}, number of queries that TTJ is the fastest: {count_ttj_fastest}")

    def ttj_speedup_analysis(baseline):
        print(f"Speedup analysis for TTJ over {baseline}")
        data_speedup = dict()
        baseline = grouped_data[baseline]
        data_speedup[TTJ] = \
            [round(baseline_time / algorithm_time, 1) for baseline_time, algorithm_time in zip(baseline, grouped_data[TTJ])]
        speedup_analysis(data_speedup, labels)

    ttj_speedup_analysis(HJ)
    ttj_speedup_analysis(Yannakakis1Pass)


def tpch_speedup_with_predicates():
    tpch_plot_with_predicates = {
        DATA_SOURCE_CSV: "benchmarktpchwithpredicateshjorderingshallow-result-2024-07-15t01:00:56.113842benchmarktpchwithpredicateshjorderingshallow-result-2024-07-20t16:41:53.030291_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis1Pass, SQLITE],
        COLUMN_RIGHT_BOUND: 14,
    }

    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        # patterns = ["|", "\\", "/", "+", "-", ".", "*", "x", "o", "O"]
        patterns = ["\\", "-", "/", "", "*"]

        # Check if colors where provided, otherwhise use the default color cycle
        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

        # Number of bars per group
        n_bars = len(data)

        # The width of a single bar
        bar_width = total_width / n_bars

        # List containing handles for the drawn bars, used for the legend
        bars = []

        # Iterate over all data
        for i, name in enumerate([Yannakakis1Pass, HJ, TTJ]):
            values = data[name]
            # The offset in x direction of that bar
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2

            # Draw a bar for every value of that type
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
                # The following is about speedup
                # if name == HJ:
                #     plt.text(x=x + x_offset, y=y+1, s=f"{speedup[x]}", fontdict=fontdict,
                #              rotation=90)

            # Add a handle to the last drawn bar, which we'll need for the legend
            bars.append(bar[0])

        # Draw legend if we need
        if legend:
            ax.legend(bars, [r'$\mathsf{YA}^+$', r"$\mathsf{HJ}$", r'$\mathsf{TTJ}$'], fontsize=15, ncol=3, frameon=False,
                      loc='best')

        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        ax.set_yscale('log')
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 10}
        ax.text(0, 1.7, r'$\mathsf{SQLite}$', color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    prem_data, _ = extract_data_from_csv(get_tpch_full_path(tpch_plot_with_predicates[DATA_SOURCE_CSV]),
                                               column_range=[1, tpch_plot_with_predicates[COLUMN_RIGHT_BOUND]])
    prem_data[SQLITE] = process_tpch_raw_data()
    labels = ["3", "7", "8", "9", "10", "11", "12", "14", "15", "16", "18", "19", "20"]

    check_argument(len(prem_data.keys()) >= len(tpch_plot_with_predicates[ALGORITHMS_TO_PLOT]),
                   f"there should be {len(tpch_plot_with_predicates[ALGORITHMS_TO_PLOT])} algorithms in data. Current only have {prem_data.keys()}")
    for dps in prem_data.values():
        check_argument(len(dps) == len(labels),
                       f"some query data is missing. There should be {len(labels)} dps. Instead, we have {len(dps)}")

    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in tpch_plot_with_predicates[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    data_speedup = dict()
    for algorithm, dps in data.items():
        sqlite = data[SQLITE]
        data_speedup[algorithm] = \
            [round(sqlite_time / algorithm_time, 1) for sqlite_time, algorithm_time in zip(sqlite, data[algorithm])]
    del data_speedup[SQLITE]

    speedup_analysis(data_speedup, labels)
    ttj_perf_analysis(prem_data, labels)

    plt.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 3), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    bar_plot(ax, data_speedup, colors=[Yannakakis1Pass_COLOR, HJ_COLOR, TTJ_COLOR], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.ylabel('Speedup (log scale)', fontdict=font2)
    plt.margins(x=0.01)
    plt.tight_layout()
    filename = "exp1.1.tpch.2-sqlite.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    tpch_speedup_with_predicates()