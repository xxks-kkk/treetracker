"""
Experiment 1.1: Distribution of individual query evaluation time

TPC-H bar graph (including SQLite performance)
"""
import logging
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt, transforms, ticker
from scipy.stats import gmean

from icde2025.postgres_perf import process_tpch_enforced_raw_data
from plot.constants import DATA_SOURCE_CSV, HJ, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, TTJ, Yannakakis1Pass, TTJ_COLOR, \
    Yannakakis1Pass_COLOR, HJ_COLOR, POSTGRES
from plot.job import extract_data_from_csv
from plot.utility import check_argument

logging.getLogger('matplotlib.font_manager').disabled = True


def get_tpch_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "tpch" / "with_predicates" / "hj_ordering_hj" / csv_name


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

    ttj_fastest = np.array([])
    next_best = np.array([])

    ttj_next_best = np.array([])
    best_when_ttj_is_second = np.array([])
    ttj_slowest = np.array([])
    best = np.array([])

    ttj_slower_hj = np.array([])
    ttj_slower_hj_hj = np.array([])

    for idx in range(len(grouped_data[TTJ])):
        if grouped_data[TTJ][idx] < grouped_data[HJ][idx] and \
                grouped_data[TTJ][idx] < grouped_data[Yannakakis1Pass][idx]:
            count_ttj_fastest += 1
            ttj_fastest = np.append(ttj_fastest, grouped_data[TTJ][idx])
            if grouped_data[HJ][idx] < grouped_data[Yannakakis1Pass][idx]:
                next_best = np.append(next_best, grouped_data[HJ][idx])
            else:
                next_best = np.append(next_best, grouped_data[Yannakakis1Pass][idx])
        else:
            if grouped_data[TTJ][idx] < grouped_data[HJ][idx] and grouped_data[TTJ][idx] > \
                    grouped_data[Yannakakis1Pass][idx]:
                ttj_next_best = np.append(ttj_next_best, grouped_data[TTJ][idx])
                best_when_ttj_is_second = np.append(best_when_ttj_is_second, grouped_data[Yannakakis1Pass][idx])
            elif grouped_data[TTJ][idx] > grouped_data[HJ][idx] and grouped_data[TTJ][idx] < \
                    grouped_data[Yannakakis1Pass][idx]:
                ttj_next_best = np.append(ttj_next_best, grouped_data[TTJ][idx])
                best_when_ttj_is_second = np.append(best_when_ttj_is_second, grouped_data[HJ][idx])
            elif grouped_data[TTJ][idx] > grouped_data[HJ][idx] and grouped_data[TTJ][idx] > \
                    grouped_data[Yannakakis1Pass][idx]:
                ttj_slowest = np.append(ttj_slowest, grouped_data[TTJ][idx])
                if grouped_data[HJ][idx] > grouped_data[Yannakakis1Pass][idx]:
                    best = np.append(best, grouped_data[Yannakakis1Pass][idx])
                else:
                    best = np.append(best, grouped_data[HJ][idx])
        if grouped_data[TTJ][idx] > grouped_data[HJ][idx]:
            ttj_slower_hj = np.append(ttj_slower_hj, grouped_data[TTJ][idx])
            ttj_slower_hj_hj = np.append(ttj_slower_hj_hj, grouped_data[HJ][idx])

    print("analyze the margin of TTJ being the fast algorithm ...")
    ttj_fastest_speedup = \
        [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
         zip(next_best, ttj_fastest)]
    ttj_fastest_speedup.sort(reverse=True)
    print(f"ttj_fastest_speedup ({len(ttj_fastest_speedup)}): {ttj_fastest_speedup}")
    speedup_below_one_percent_count = 0
    speedup_below_ten_percent_count = 0
    for speedup in ttj_fastest_speedup:
        if speedup < 0.01:
            speedup_below_one_percent_count += 1
        if speedup < 0.1:
            speedup_below_ten_percent_count += 1
    print(f"speedup_below_one_percent_count: {speedup_below_one_percent_count}")
    print(f"speedup_below_ten_percent_count: {speedup_below_ten_percent_count}")

    print(f"total queries: {total_queries}, number of queries that TTJ is the fastest: {count_ttj_fastest}")

    print("analyze TTJ slower than HJ queries ...")
    ttj_slower_hj_percentage = [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
                                zip(ttj_slower_hj_hj, ttj_slower_hj)]
    print(f"ttj_slower_hj_percentage: {ttj_slower_hj_percentage}")
    print(f"the min of ttj_slower_hj_percentage: {min(ttj_slower_hj_percentage)}")
    ttj_slower_hj_percentage_positive = [-1 * val for val in ttj_slower_hj_percentage]
    print(f"geometric mean of ttj_slower_hj_percentage: {gmean(ttj_slower_hj_percentage_positive)}")

    print("analyze the margin of TTJ being the next best algorithm ...")
    speedup_below_one_percent_next_best_count = 0
    speedup_below_ten_percent_next_best_count = 0
    ttj_next_best_speedup = \
        [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
         zip(best_when_ttj_is_second, ttj_next_best)]
    ttj_next_best_speedup.sort(reverse=True)
    print(f"ttj_next_best_speedup ({len(ttj_next_best_speedup)}): {ttj_next_best_speedup}")
    for speedup in ttj_next_best_speedup:
        if speedup > -0.01:
            speedup_below_one_percent_next_best_count += 1
        if speedup > -0.1:
            speedup_below_ten_percent_next_best_count += 1
    print(f"speedup_below_one_percent_next_best_count: {speedup_below_one_percent_next_best_count}")
    print(f"speedup_below_ten_percent_next_best_count: {speedup_below_ten_percent_next_best_count}")

    print("analyze the margin of TTJ being the slowest algorithm ...")
    speedup_below_one_percent_slowest_count = 0
    speedup_below_ten_percent_slowest_count = 0
    ttj_slowest_speedup = \
        [(baseline_time - algorithm_time) / baseline_time for baseline_time, algorithm_time in
         zip(best, ttj_slowest)]
    ttj_slowest_speedup.sort(reverse=True)
    print(f"ttj_slowest_speedup ({len(ttj_slowest_speedup)}): {ttj_slowest_speedup}")
    for speedup in ttj_slowest_speedup:
        if speedup > -0.01:
            speedup_below_one_percent_slowest_count += 1
        if speedup > -0.1:
            speedup_below_ten_percent_slowest_count += 1
    print(f"speedup_below_one_percent_slowest_count: {speedup_below_one_percent_slowest_count}")
    print(f"speedup_below_ten_percent_slowest_count: {speedup_below_ten_percent_slowest_count}")

    def ttj_speedup_analysis(baseline):
        print(f"Speedup analysis for TTJ over {baseline}")
        data_speedup = dict()
        baseline = grouped_data[baseline]
        data_speedup[TTJ] = \
            [round(baseline_time / algorithm_time, 1) for baseline_time, algorithm_time in
             zip(baseline, grouped_data[TTJ])]
        speedup_analysis(data_speedup, labels)

    ttj_speedup_analysis(HJ)
    ttj_speedup_analysis(Yannakakis1Pass)


def tpch_speedup_with_predicates():
    tpch_plot_with_predicates = {
        DATA_SOURCE_CSV: "2024-12-06T00:39:22.882486Z_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis1Pass, POSTGRES],
        COLUMN_RIGHT_BOUND: 14,
    }

    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
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
        for i, name in enumerate([TTJ, HJ, Yannakakis1Pass]):
            values = data[name]
            # The offset in x direction of that bar
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2

            # Draw a bar for every value of that type
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)])
                # The following is about speedup
                # if name == HJ:
                #     plt.text(x=x + x_offset, y=y+1, s=f"{speedup[x]}", fontdict=fontdict,
                #              rotation=90)

            # Add a handle to the last drawn bar, which we'll need for the legend
            bars.append(bar[0])

        # Draw legend if we need
        if legend:
            bars.append(ax.axhline(y=1, color='#000000', linestyle='-', linewidth=1))
            ax.legend(bars, [r'$\mathsf{TTJ}$', r"$\mathsf{HJ}$", r'$\mathsf{YA}$', 'Postgres'], fontsize=20, ncol=7,
                      frameon=False,
                      loc='center', bbox_to_anchor=(0.54, 0.9))

        ax.set_yscale('log')
        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 10}
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    prem_data, _ = extract_data_from_csv(get_tpch_full_path(tpch_plot_with_predicates[DATA_SOURCE_CSV]),
                                         column_range=[1, tpch_plot_with_predicates[COLUMN_RIGHT_BOUND]])
    prem_data[POSTGRES] = process_tpch_enforced_raw_data()
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
        postgres = data[POSTGRES]
        data_speedup[algorithm] = \
            [round(postgres_time / algorithm_time, 1) for postgres_time, algorithm_time in
             zip(postgres, data[algorithm])]
    del data_speedup[POSTGRES]

    speedup_analysis(data_speedup, labels)
    ttj_perf_analysis(prem_data, labels)

    plt.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 4), dpi=300)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 20}
    bar_plot(ax, data_speedup, colors=[TTJ_COLOR, HJ_COLOR, Yannakakis1Pass_COLOR], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 17}
    plt.ylabel('Speedup (log scale)', fontdict=font2)
    plt.margins(x=0.01)
    plt.tight_layout()
    filename = "exp1.1.tpch.2-postgres.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    tpch_speedup_with_predicates()
