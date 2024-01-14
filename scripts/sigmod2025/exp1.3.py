"""
Exp 1.3: Runtime breakdown

Bar plot with reduction time %
"""
import csv
from collections import OrderedDict
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import DATA_SOURCE_CSV, TTJ, HJ, Yannakakis, LIP, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT
from plot.job import extract_data_from_csv
from plot.utility import to_float

noGoodListProbingTime="noGoodListProbingTime"
noGoodListConstructTime="noGoodListConstructTime"
deleteDanglingTupleFromHTime="deleteDanglingTupleFromHTime"
fullReducerTime="fullReducerTime"
buildBloomFiltersTime="buildBloomFiltersTime"
bloomFiltersProbingTime="bloomFiltersProbingTime"

def get_ssb_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "simple-cost-model-with-predicates" / csv_name

def get_ssb_perf_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "ssb" / csv_name

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
                if noGoodListProbingTime in row:
                    result[algorithm][noGoodListProbingTime] = [to_float(num) for num in row[1:]]
                elif noGoodListConstructTime in row:
                    result[algorithm][noGoodListConstructTime] = [to_float(num) for num in row[1:]]
                elif deleteDanglingTupleFromHTime in row:
                    result[algorithm][deleteDanglingTupleFromHTime] = [to_float(num) for num in row[1:]]
                elif fullReducerTime in row:
                    result[algorithm][fullReducerTime] = [to_float(num) for num in row[1:]]
                elif buildBloomFiltersTime in row:
                    result[algorithm][buildBloomFiltersTime] = [to_float(num) for num in row[1:]]
                elif bloomFiltersProbingTime in row:
                    result[algorithm][bloomFiltersProbingTime] = [to_float(num) for num in row[1:]]
    return result

def plot_ssb():
    def bar_plot(ax, data, labels, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
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
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'Yannakakis', r'$\mathsf{LIP}$'], frameon=False, fontsize=20, ncol=3,
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
                'size': 20}
        # ax.text(0.05, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
        #         ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        ax.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    conf = {
        DATA_SOURCE_CSV: {TTJ: "TTJHP_SSB_aggregagateStatistics.csv",
                          HJ: "HASH_JOIN_SSB_aggregagateStatistics.csv",
                          Yannakakis : "Yannakakis_SSB_aggregagateStatistics.csv",
                          LIP : "LIP_SSB_aggregagateStatistics.csv"},
        COLUMN_RIGHT_BOUND: 14,
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis, LIP],
    }
    agg_data = extract_data_from_agg_csv(conf[DATA_SOURCE_CSV], get_ssb_full_path)
    prem_data, _ = extract_data_from_csv(get_ssb_perf_full_path("benchmarkssb-result-2023-10-26t12:14:04.768421_perf_report.csv"),
                                               column_range=[1, conf[COLUMN_RIGHT_BOUND]])
    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in conf[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    reduction_time = dict()
    for algorithm in agg_data:
        reduction_time[algorithm] = []
        for i in range(len(agg_data[algorithm][noGoodListConstructTime])):
            reduction_time_i = 0
            for time in agg_data[algorithm]:
                reduction_time_i += agg_data[algorithm][time][i]
            reduction_time[algorithm].append(reduction_time_i)

    reduction_time_percentage = dict()
    for algorithm in data:
        reduction_time_percentage[algorithm] = []
        for i in range(len(data[algorithm])):
            reduction_time_percentage[algorithm].append(reduction_time[algorithm][i] / data[algorithm][i])
    del reduction_time_percentage[HJ]

    reduction_time_percentage_use = dict()
    for algorithm in [TTJ, Yannakakis, LIP]:
        reduction_time_percentage_use[algorithm] = reduction_time_percentage[algorithm]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(1, 1, figsize=(14, 14), dpi=140)
    labels = ["1.1", "1.2", "1.3", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3"]


    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(ax, reduction_time_percentage_use, labels=labels, colors=['#00994D', '#FF9933', '#9933FF'], total_width=.7,
             single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    ax.set_ylabel('reduction time %', fontdict=font2)
    plt.tight_layout()
    filename = "exp1.3.ssb.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()

if __name__ == "__main__":
    plot_ssb()