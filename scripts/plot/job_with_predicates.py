"""
Draw JOB performance with predicates
"""

from pathlib import Path
from typing import List

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import ticker, transforms

from plot.constants import DATA_SOURCE_CSV, HJ, TTJ, Yannakakis, FIG_SAVE_LOCATION, DECIMAL_PRECISION, \
    PLOT_FUNCTION, ALGORITHMS_TO_PLOT, LIP, PERCENTILES, COLUMN_RIGHT_BOUND, ENABLE_SPEEDUP
from plot.cost_model4 import extract_data_from_csv
from plot.job import construct_fig_name
from plot.utility import check_argument, TimeUnits, convert_time


def job_speedup(plot_conf: dict):
    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
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
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{LIP}$',
                             r'Yannakakis'], frameon=False, fontsize=20, ncol=3,
                      loc=(0.5,0.7))

        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 20}
        ax.text(0.05, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
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
            grouped_data[algorithm].append(data['q1a'] + data['q1b'] + data['q1c'] + data['q1d'])
            grouped_data[algorithm].append(data['q2a'] + data['q2b'] + data['q2c'] + data['q2d'])
            grouped_data[algorithm].append(data['q3a'] + data['q3b'] + data['q3c'])
            grouped_data[algorithm].append(data['q4a'] + data['q4b'] + data['q4c'])
            grouped_data[algorithm].append(data['q5a'] + data['q5b'] + data['q5c'])
            grouped_data[algorithm].append(data['q6a'] + data['q6b'] + data['q6c'] + data['q6d'] + data['q6e'] + data['q6f'])
            grouped_data[algorithm].append(data['q7a'] + data['q7b'] + data['q7c'])
            grouped_data[algorithm].append(data['q8a'] + data['q8b'] + data['q8c'] + data['q8d'])
            grouped_data[algorithm].append(data['q9a'] + data['q9b'] + data['q9c'] + data['q9d'])
            grouped_data[algorithm].append(data['q10a'] + data['q10b'] + data['q10c'])
            grouped_data[algorithm].append(data['q11a'] + data['q11b'] + data['q11c'] + data['q11d'])
            grouped_data[algorithm].append(data['q12a'] + data['q12b'] + data['q12c'])
            grouped_data[algorithm].append(data['q13a'] + data['q13b'] + data['q13c'] + data['q13d'])
            grouped_data[algorithm].append(data['q14a'] + data['q14b'] + data['q14c'])
            grouped_data[algorithm].append(data['q15a'] + data['q15b'] + data['q15c'] + data['q15d'])
            grouped_data[algorithm].append(data['q16a'] + data['q16b'] + data['q16c'] + data['q16d'])
            grouped_data[algorithm].append(data['q17a'] + data['q17b'] + data['q17c'] + data['q17d'] + data['q17e'] + data['q17f'])
            grouped_data[algorithm].append(data['q18a'] + data['q18b'] + data['q18c'])
            grouped_data[algorithm].append(data['q19a'] + data['q19b'] + data['q19c'] + data['q19d'])
            grouped_data[algorithm].append(data['q20a'] + data['q20b'] + data['q20c'])
            grouped_data[algorithm].append(data['q21a'] + data['q21b'] + data['q21c'])
            grouped_data[algorithm].append(data['q22a'] + data['q22b'] + data['q22c'] + data['q22d'])
            grouped_data[algorithm].append(data['q23a'] + data['q23b'] + data['q23c'])
            grouped_data[algorithm].append(data['q24a'] + data['q24b'])
            grouped_data[algorithm].append(data['q25a'] + data['q25b'] + data['q25c'])
            grouped_data[algorithm].append(data['q26a'] + data['q26b'] + data['q26c'])
            grouped_data[algorithm].append(data['q27a'] + data['q27b'] + data['q27c'])
            grouped_data[algorithm].append(data['q28a'] + data['q28b'] + data['q28c'])
            grouped_data[algorithm].append(data['q29a'] + data['q29b'] + data['q29c'])
            grouped_data[algorithm].append(data['q30a'] + data['q30b'] + data['q30c'])
            grouped_data[algorithm].append(data['q31a'] + data['q31b'] + data['q31c'])
            grouped_data[algorithm].append(data['q32a'] + data['q32b'])
            grouped_data[algorithm].append(data['q33a'] + data['q33b'] + data['q33c'])
        return grouped_data

    data = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]),
                                 column_range=[1, 114])
    check_argument(len(data.keys()) == 4, f"there should be 4 algorithms in data. Current only have {data.keys()}")
    for dps in data.values():
        check_argument(len(dps) == 113,
                       f"some query data is missing. There should be 113 dps. Instead, we have {len(dps)}")

    labels = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30",
              "Q31", "Q32", "Q33"]
    grouped_data = perform_grouping(data)

    original_idx = None
    for algorithm in grouped_data.keys():
        if algorithm == HJ:
            original_idx = np.array(grouped_data[algorithm]).argsort()[::-1]
            grouped_data[algorithm] = sorted(grouped_data[algorithm], reverse=True)
        else:
            grouped_data[algorithm] = [grouped_data[algorithm][i] for i in original_idx]
    labels = [labels[i] for i in original_idx]

    data_speedup = dict()
    for algorithm, dps in grouped_data.items():
        hj = grouped_data[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, grouped_data[algorithm])]
    del data_speedup[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 3), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    bar_plot(ax, data_speedup, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.ylabel('Speedup', fontdict=font2)
    plt.tight_layout()
    filename = "job_speedup_with_predicates_sorted.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def job_perf_improvements(plot_conf: dict):
    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
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
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{Yannakakis}$'], frameon=False, fontsize=15, ncol=3,
                      loc=(0.8,0.8))

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
                'size': 20}
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        ax.text(0.05, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
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

    prem_data = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]),
                                 column_range=[1, plot_conf[COLUMN_RIGHT_BOUND]])
    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in plot_conf[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    for dps in data.values():
        check_argument(len(dps) == plot_conf[COLUMN_RIGHT_BOUND] - 1,
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
        if plot_conf[ENABLE_SPEEDUP]:
            data_speedup[algorithm] = \
                [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, grouped_data[algorithm])]
        else:
            data_speedup[algorithm] = \
                [round(algorithm_time / hj_time, 1) for hj_time, algorithm_time in zip(hj, grouped_data[algorithm])]
    del data_speedup[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 3), dpi=140)
    font = {'family': 'Helvetica',
            'size': 5}
    bar_plot(ax, data_speedup, colors=['#00994D', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    if plot_conf[ENABLE_SPEEDUP]:
        plt.ylabel('Speedup', fontdict=font2)
    else:
        plt.ylabel('Slowdown', fontdict=font2)
    plt.xticks(rotation=270)
    plt.tight_layout()
    filename = "job_with_predicates.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def plot_execution_time_breakddown_by_query_expense(plot_conf: dict):
    """
    Like Ding2020 Figure 8, we classify queries into cheap queries, normal queries, and expensive queries using hash join.
    And we break down execution time by these three categories.
    """

    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None,
                 add_bar_label=True):
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
                if add_bar_label:
                    ax.bar_label(bar, label_type='edge', padding=3, fontsize=20)
            bars.append(bar[0])

        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{LIP}$',
                             r'Yannakakis'], frameon=False, fontsize=20, ncol=1,
                      loc=(0.76, 0.45))

        ax.set_yscale('log')
        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 20}
        ax.text(0.05, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        ax.xaxis.set_tick_params(labelsize='xx-large')
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    raw_data = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]), column_range=[1,114])
    expected_dps_len = 113

    def perform_grouping(raw_data: dict) -> dict:
        """
        We aggregate all the queries with the same flight, e.g., 1a, 1b, 1c together
        """
        grouped_data = dict()
        for algorithm in raw_data.keys():
            grouped_data[algorithm] = []
            data = raw_data[algorithm]
            grouped_data[algorithm].extend(data.values())
        return grouped_data

    data = perform_grouping(raw_data)

    plot_data = dict()
    labels = ['Expensive queries', 'Normal queries', 'Cheap queries']
    expensive_queres = dict()
    for algorithm, dps in data.items():
        expensive_queres[algorithm] = dps[:int(expected_dps_len / 3)]
        if algorithm not in plot_data:
            plot_data[algorithm] = []
        plot_data[algorithm].append(sum(expensive_queres[algorithm]))

    middle_queries = dict()
    for algorithm, dps in data.items():
        middle_queries[algorithm] = dps[int(expected_dps_len / 3): int(expected_dps_len / 3 * 2)]
        if algorithm not in plot_data:
            plot_data[algorithm] = []
        plot_data[algorithm].append(sum(middle_queries[algorithm]))

    cheap_queries = dict()
    for algorithm, dps in data.items():
        cheap_queries[algorithm] = dps[int(expected_dps_len / 3 * 2):]
        if algorithm not in plot_data:
            plot_data[algorithm] = []
        plot_data[algorithm].append(sum(cheap_queries[algorithm]))

    data_speedup = dict()
    for algorithm, dps in plot_data.items():
        hj = plot_data[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, plot_data[algorithm])]
    del data_speedup[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(10, 3), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 20}
    bar_plot(ax, data_speedup, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.ylabel('Speedup', fontdict=font2)
    plt.tight_layout()
    filename = "execution_time_breakddown_by_query_expense2_with_predicates.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def compute_perecentile_of_speedup(plot_conf: dict):
    """
    We compute the 25%, 50%, 75% speedup of TTJ compared to the rest three algorithms in JOB with predicates
    """
    raw_data = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]), column_range=[1,114])
    speedup_ttj_hj = 'speedup_ttj_hj'
    speedup_ttj_lip = 'speedup_ttj_lip'
    speedup_ttj_yannakakis = 'speedup_ttj_yannakakis'

    distribution_of_speedup = dict()
    for algorithm, dps in raw_data.items():
        for query_label, value in dps.items():
            if speedup_ttj_hj not in distribution_of_speedup:
                distribution_of_speedup[speedup_ttj_hj] = []
            distribution_of_speedup[speedup_ttj_hj].append(raw_data[HJ][query_label] / raw_data[TTJ][query_label])
            if speedup_ttj_lip not in distribution_of_speedup:
                distribution_of_speedup[speedup_ttj_lip] = []
            distribution_of_speedup[speedup_ttj_lip].append(raw_data[LIP][query_label] / raw_data[TTJ][query_label])
            if speedup_ttj_yannakakis not in distribution_of_speedup:
                distribution_of_speedup[speedup_ttj_yannakakis] = []
            distribution_of_speedup[speedup_ttj_yannakakis].append(raw_data[Yannakakis][query_label] / raw_data[TTJ][query_label])
        break

    percentiles = dict()
    for speedup_ttj_algorithm, dps in distribution_of_speedup.items():
        if speedup_ttj_algorithm not in percentiles:
            percentiles[speedup_ttj_algorithm] = dict()
        for perecentile in plot_conf[PERCENTILES]:
            percentiles[speedup_ttj_algorithm][perecentile] = np.percentile(np.array(dps), perecentile)
    print(percentiles)


def driver(plot_conf: dict):
    """
    Invoke the necessary plotting function
    """
    for func in plot_conf[PLOT_FUNCTION]:
        func(plot_conf)


if __name__ == "__main__":
    job_plot = {
        DATA_SOURCE_CSV: "2023-08-03T00:01:33.555478Z_perf_report.csv",
        PLOT_FUNCTION: [job_perf_improvements],
        FIG_SAVE_LOCATION: r".",
        ALGORITHMS_TO_PLOT: [HJ, TTJ],
        PERCENTILES: [25, 50, 75, 95],
        COLUMN_RIGHT_BOUND: 114,
        ENABLE_SPEEDUP: True,
    }
    driver(plot_conf=job_plot)
