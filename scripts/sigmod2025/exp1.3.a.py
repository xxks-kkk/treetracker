"""
Exp1.3: We compare join time vs. preprocessing time across all algorithms
"""
import csv
from collections import OrderedDict
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import DATA_SOURCE_CSV, TTJ, HJ, Yannakakis, LIP, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, \
    YannakakisB
from plot.job import extract_data_from_csv
from plot.utility import to_float

main_sample_count = "main_sample_count"
join_fragment_eval_count = "join_fragment_eval_count"
cursor_count = "cursor_count"
bloom_filter_build_count = "bloom_filter_build_count"
bloom_filter_probe_count = "bloom_filter_probe_count"
ng_probing_count = "ng_probing_count"
ng_construct_count = "ng_construct_count"
remove_dangling_tuple_from_rinner_count = "remove_dangling_tuple_from_rinner_count"
full_reducer_count="full_reducer_count"


def get_profiling_results_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "profiling" / csv_name


def extract_data_from_agg_csv(DATA_SOURCE_CSV, get_full_path_func):
    full_data_source_csv = dict()
    for algorithm, csvfile in DATA_SOURCE_CSV.items():
        full_data_source_csv[algorithm] = get_full_path_func(csvfile)
    result = dict()
    for algorithm, csv_file_path in full_data_source_csv.items():
        result[algorithm] = dict()
        with open(csv_file_path, "r") as file:
            csv_file = csv.reader(file)
            headers = next(csv_file)
            for row in csv_file:
                if join_fragment_eval_count in row:
                    result[algorithm][join_fragment_eval_count] = [to_float(num) for num in row[1:]]
                elif bloom_filter_build_count in row:
                    result[algorithm][bloom_filter_build_count] = [to_float(num) for num in row[1:]]
                elif bloom_filter_probe_count in row:
                    result[algorithm][bloom_filter_probe_count] = [to_float(num) for num in row[1:]]
                elif ng_probing_count in row:
                    result[algorithm][ng_probing_count] = [to_float(num) for num in row[1:]]
                elif ng_construct_count in row:
                    result[algorithm][ng_construct_count] = [to_float(num) for num in row[1:]]
                elif remove_dangling_tuple_from_rinner_count in row:
                    result[algorithm][remove_dangling_tuple_from_rinner_count] = [to_float(num) for num in row[1:]]
                elif full_reducer_count in row:
                    result[algorithm][full_reducer_count] = [to_float(num) for num in row[1:]]
                elif cursor_count in row:
                    result[algorithm][cursor_count] = [to_float(num) for num in row[1:]]
    return result


def plot_ssb():
    def bar_plot(ax, data, labels, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        patterns = ["\\", "-", "/", ""]

        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        n_bars = len(data)
        bar_width = total_width / n_bars
        bars = []
        for i, name in enumerate([TTJ, Yannakakis, LIP, YannakakisB]):
            values = data[name]
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
            bars.append(bar[0])
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{YA}$', r'$\mathsf{LIP}$', r'$\mathsf{PT}$'], frameon=False, fontsize=10, ncol=4,
                      loc='best')

        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        # ax.axhline(y=1, color='#FF0000', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 10}
        # ax.text(0.05, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
        #         ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        ax.yaxis.set_major_formatter(ticker.PercentFormatter(1))
        ax.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    conf = {
        DATA_SOURCE_CSV: {TTJ: "ssb_TTJHP_profiling_agg.csv",
                          Yannakakis: "ssb_Yannakakis_profiling_agg.csv",
                          LIP: "ssb_LIP_profiling_agg.csv",
                          YannakakisB: "ssb_YannakakisB_profiling_agg.csv"},
    }
    agg_data = extract_data_from_agg_csv(conf[DATA_SOURCE_CSV], get_profiling_results_full_path)

    reduction_time = dict()
    for algorithm in agg_data:
        reduction_time[algorithm] = []
        for i in range(len(agg_data[algorithm][join_fragment_eval_count])):
            reduction_time_i = 0
            for time in agg_data[algorithm]:
                if time != join_fragment_eval_count and time != cursor_count:
                    reduction_time_i += agg_data[algorithm][time][i]
            reduction_time[algorithm].append(reduction_time_i)

    reduction_time_percentage = dict()
    for algorithm in agg_data:
        reduction_time_percentage[algorithm] = []
        for i in range(len(agg_data[algorithm][join_fragment_eval_count])):
            if algorithm == Yannakakis or algorithm == YannakakisB:
                reduction_time_percentage[algorithm].append((reduction_time[algorithm][i] - agg_data[algorithm][cursor_count][i]) / (agg_data[algorithm][join_fragment_eval_count][i] - agg_data[algorithm][cursor_count][i]))
            else:
                reduction_time_percentage[algorithm].append(reduction_time[algorithm][i] / agg_data[algorithm][join_fragment_eval_count][i])

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(1, 1, figsize=(6, 6), dpi=140)
    labels = ["1.1", "1.2", "1.3", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3"]

    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(ax, reduction_time_percentage, labels=labels, colors=['#00994D', '#FF9933', '#9933FF', "#00C3E3"], total_width=.7,
             single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    ax.set_ylabel('Fraction of runtime on removing dangling tuples', fontdict=font2)
    plt.tight_layout()
    filename = "exp1.3.a.ssb.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    plot_ssb()
