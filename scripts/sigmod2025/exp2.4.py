"""
Exp 2.4: TTJ Robustness against Poor Plans

TTJ on HJ ordering vs. TTJ on TTJ ordering vs. HJ on HJ ordering
"""

from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker
from scipy.stats import gmean

from plot.constants import HJ, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, DATA_SOURCE_CSV, TTJ, Yannakakis, \
    TTJ_FIXED_HJ_ORDERING
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "job" / "with_predicates" / csv_name


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


def job_speedup():
    def bar_plot(ax, data, enable_labels, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        patterns = ["\\", "-", "/", ""]

        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        n_bars = len(data)
        bar_width = total_width / n_bars
        bars = []
        for i, (name, values) in enumerate(data.items()):
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
            bars.append(bar[0])
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{TTJ}^{o}$'], frameon=False, fontsize=15, ncol=2,
                      loc='best')

        if enable_labels:
            x = np.arange(len(labels))
            if fontdict is not None:
                ax.set_xticks(x)
                ax.set_xticklabels(labels, fontproperties=fontdict)
            else:
                ax.set_xticks(x, labels)
        # plt.axhline(y=0, color='#FF0000', linestyle='-')
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 20}
        # ax.text(0.05, 0.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
        #         ha="right", va="center", fontdict=font)
        ax.text(0, 1.3, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        if not enable_labels:
            ax.set_xticklabels([])
            ax.set_xticks([])
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    def perform_grouping(raw_data: dict) -> dict:
        """
        We aggregate all the queries with the same flight, e.g., 1a, 1b, 1c together
        """
        grouped_data = dict()
        for algorithm in raw_data.keys():
            grouped_data[algorithm] = []
            data = raw_data[algorithm]
            grouped_data[algorithm].append(data['query1a'])
            grouped_data[algorithm].append(data['query1b'])
            grouped_data[algorithm].append(data['query1c'])
            grouped_data[algorithm].append(data['query1d'])
            grouped_data[algorithm].append(data['query2a'])
            grouped_data[algorithm].append(data['query2b'])
            grouped_data[algorithm].append(data['query2c'])
            grouped_data[algorithm].append(data['query2d'])
            grouped_data[algorithm].append(data['query3a'])
            grouped_data[algorithm].append(data['query3b'])
            grouped_data[algorithm].append(data['query3c'])
            grouped_data[algorithm].append(data['query4a'])
            grouped_data[algorithm].append(data['query4b'])
            grouped_data[algorithm].append(data['query4c'])
            grouped_data[algorithm].append(data['query5a'])
            grouped_data[algorithm].append(data['query5b'])
            grouped_data[algorithm].append(data['query5c'])
            grouped_data[algorithm].append(data['query6a'])
            grouped_data[algorithm].append(data['query6b'])
            grouped_data[algorithm].append(data['query6c'])
            grouped_data[algorithm].append(data['query6d'])
            grouped_data[algorithm].append(data['query6e'])
            grouped_data[algorithm].append(data['query6f'])
            grouped_data[algorithm].append(data['query7a'])
            grouped_data[algorithm].append(data['query7b'])
            grouped_data[algorithm].append(data['query7c'])
            grouped_data[algorithm].append(data['query8a'])
            grouped_data[algorithm].append(data['query8b'])
            grouped_data[algorithm].append(data['query8c'])
            grouped_data[algorithm].append(data['query8d'])
            grouped_data[algorithm].append(data['query9a'])
            grouped_data[algorithm].append(data['query9b'])
            grouped_data[algorithm].append(data['query9c'])
            grouped_data[algorithm].append(data['query9d'])
            grouped_data[algorithm].append(data['query10a'])
            grouped_data[algorithm].append(data['query10b'])
            grouped_data[algorithm].append(data['query10c'])
            grouped_data[algorithm].append(data['query11a'])
            grouped_data[algorithm].append(data['query11b'])
            grouped_data[algorithm].append(data['query11c'])
            grouped_data[algorithm].append(data['query11d'])
            grouped_data[algorithm].append(data['query12a'])
            grouped_data[algorithm].append(data['query12b'])
            grouped_data[algorithm].append(data['query12c'])
            grouped_data[algorithm].append(data['query13a'])
            grouped_data[algorithm].append(data['query13b'])
            grouped_data[algorithm].append(data['query13c'])
            grouped_data[algorithm].append(data['query13d'])
            grouped_data[algorithm].append(data['query14a'])
            grouped_data[algorithm].append(data['query14b'])
            grouped_data[algorithm].append(data['query14c'])
            grouped_data[algorithm].append(data['query15a'])
            grouped_data[algorithm].append(data['query15b'])
            grouped_data[algorithm].append(data['query15c'])
            grouped_data[algorithm].append(data['query15d'])
            grouped_data[algorithm].append(data['query16a'])
            grouped_data[algorithm].append(data['query16b'])
            grouped_data[algorithm].append(data['query16c'])
            grouped_data[algorithm].append(data['query16d'])
            grouped_data[algorithm].append(data['query17a'])
            grouped_data[algorithm].append(data['query17b'])
            grouped_data[algorithm].append(data['query17c'])
            grouped_data[algorithm].append(data['query17d'])
            grouped_data[algorithm].append(data['query17e'])
            grouped_data[algorithm].append(data['query17f'])
            grouped_data[algorithm].append(data['query18a'])
            grouped_data[algorithm].append(data['query18b'])
            grouped_data[algorithm].append(data['query18c'])
            grouped_data[algorithm].append(data['query19a'])
            grouped_data[algorithm].append(data['query19b'])
            grouped_data[algorithm].append(data['query19c'])
            grouped_data[algorithm].append(data['query19d'])
            grouped_data[algorithm].append(data['query20a'])
            grouped_data[algorithm].append(data['query20b'])
            grouped_data[algorithm].append(data['query20c'])
            grouped_data[algorithm].append(data['query21a'])
            grouped_data[algorithm].append(data['query21b'])
            grouped_data[algorithm].append(data['query21c'])
            grouped_data[algorithm].append(data['query22a'])
            grouped_data[algorithm].append(data['query22b'])
            grouped_data[algorithm].append(data['query22c'])
            grouped_data[algorithm].append(data['query22d'])
            grouped_data[algorithm].append(data['query23a'])
            grouped_data[algorithm].append(data['query23b'])
            grouped_data[algorithm].append(data['query23c'])
            grouped_data[algorithm].append(data['query24a'])
            grouped_data[algorithm].append(data['query24b'])
            grouped_data[algorithm].append(data['query25a'])
            grouped_data[algorithm].append(data['query25b'])
            grouped_data[algorithm].append(data['query25c'])
            grouped_data[algorithm].append(data['query26a'])
            grouped_data[algorithm].append(data['query26b'])
            grouped_data[algorithm].append(data['query26c'])
            grouped_data[algorithm].append(data['query27a'])
            grouped_data[algorithm].append(data['query27b'])
            grouped_data[algorithm].append(data['query27c'])
            grouped_data[algorithm].append(data['query28a'])
            grouped_data[algorithm].append(data['query28b'])
            grouped_data[algorithm].append(data['query28c'])
            grouped_data[algorithm].append(data['query29a'])
            grouped_data[algorithm].append(data['query29b'])
            grouped_data[algorithm].append(data['query29c'])
            grouped_data[algorithm].append(data['query30a'])
            grouped_data[algorithm].append(data['query30b'])
            grouped_data[algorithm].append(data['query30c'])
            grouped_data[algorithm].append(data['query31a'])
            grouped_data[algorithm].append(data['query31b'])
            grouped_data[algorithm].append(data['query31c'])
            grouped_data[algorithm].append(data['query32a'])
            grouped_data[algorithm].append(data['query32b'])
            grouped_data[algorithm].append(data['query33a'])
            grouped_data[algorithm].append(data['query33b'])
            grouped_data[algorithm].append(data['query33c'])
        return grouped_data

    # CSV for TTJHP on fixed HJ ordering
    job_plot_HJ_ordering = {
        DATA_SOURCE_CSV: "benchmarkjobwithpredicatesdifferentordering-result-2023-12-30t16:43:27.406285benchmarkjobwithpredicatesfixedhjordering-result-2024-01-07t00:12:12.344309_perf_report.csv",
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data_HJ_ordering = extract_data_from_csv(get_job_full_path(job_plot_HJ_ordering[DATA_SOURCE_CSV]),
                                                  column_range=[1, job_plot_HJ_ordering[COLUMN_RIGHT_BOUND]])
    # CSV for TTJHP on different ordering (copied from exp1.1.c.py)
    job_plot = {
        DATA_SOURCE_CSV: "2024-01-04T23:06:13.391777Z_perf_report.csv",
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])

    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in [HJ, TTJ]:
            data[algorithm] = prem_data[algorithm]
    data[TTJ_FIXED_HJ_ORDERING] = prem_data_HJ_ordering[TTJ]

    for dps in data.values():
        check_argument(len(dps) == job_plot[COLUMN_RIGHT_BOUND] - 1,
                       f"some query data is missing. There should be 113 dps. Instead, we have {len(dps)}")
    labels = ["1a", "1b", "1c", "1d",
              "2a", "2b", "2c", "2d",
              "3a", "3b", "3c",
              "4a", "4b", "4c",
              "5a", "5b", "5c",
              "6a", "6b", "6c", "6d", "6e", "6f",
              "7a", "7b", "7c",
              "8a", "8b", "8c", "8d",
              "9a", "9b", "9c", "9d",
              "10a", "10b", "10c",
              "11a", "11b", "11c", "11d",
              "12a", "12b", "12c",
              "13a", "13b", "13c", "13d",
              "14a", "14b", "14c",
              "15a", "15b", "15c", "15d",
              "16a", "16b", "16c", "16d",
              "17a", "17b", "17c", "17d", "17e", "17f",
              "18a", "18b", "18c",
              "19a", "19b", "19c", "19d",
              "20a", "20b", "20c",
              "21a", "21b", "21c",
              "22a", "22b", "22c", "22d",
              "23a", "23b", "23c",
              "24a", "24b",
              "25a", "25b", "25c",
              "26a", "26b", "26c",
              "27a", "27b", "27c",
              "28a", "28b", "28c",
              "29a", "29b", "29c",
              "30a", "30b", "30c",
              "31a", "31b", "31c",
              "32a", "32b",
              "33a", "33b", "33c"]
    grouped_data = perform_grouping(data)

    data_speedup = dict()
    for algorithm, dps in grouped_data.items():
        hj = grouped_data[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, grouped_data[algorithm])]
    del data_speedup[HJ]

    speedup_analysis(data_speedup, labels)

    # data_speedup = dict()
    # for algorithm, dps in grouped_data.items():
    #     hj = grouped_data[HJ]
    #     data_speedup[algorithm] = \
    #         [(hj_time - algorithm_time) / hj_time for hj_time, algorithm_time in zip(hj, grouped_data[algorithm])]
    # del data_speedup[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 3), dpi=140)
    font = {'family': 'Helvetica',
            'size': 5}
    bar_plot(ax, data_speedup, enable_labels=False, colors=['#00994D', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 10}
    # plt.ylabel('Perf. Improvement %', fontdict=font2)
    plt.ylabel('Speedup', fontdict=font2)
    plt.margins(x=0.01)
    plt.xticks(rotation=270)
    plt.tight_layout()
    filename = "exp2.4-job.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()

if __name__ == "__main__":
    job_speedup()