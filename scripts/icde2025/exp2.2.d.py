"""
Experiment: Total intermediate results produced

The scatter plot with x-axis: input size after evaluation; y-axis: intermediate result size produced.
Each query has a scatter subplot.
"""
import csv
import re
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import ticker, transforms

from plot.constants import DATA_SOURCE_CSV, HJ, TTJ, Yannakakis, LIP, YannakakisB, Yannakakis1Pass, TTJ_COLOR, \
    Yannakakis1Pass_COLOR
from plot.cost_model4 import extract_data_from_csv
from plot.utility import to_float

input_size_after_evaluation = "input_size_after_evaluation"
total_intermediate_results_produced = "total_intermediate_results_produced"


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "simple-cost-model-with-predicates" / "hj_ordering_hj" / csv_name


def extract_number_from_str(s):
    return re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s)


def cleanup_csv(DATA_SOURCE_CSV, get_full_path_func):
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
                if "totalIntermediateResultsProduced" in row:
                    result[algorithm][total_intermediate_results_produced] = [to_float(num) for num in row[1:]]
                elif "totalInputSizeAfterEvaluation" in row:
                    result[algorithm][input_size_after_evaluation] = [to_float(num) for num in row[1:]]
    return result


def processing():
    conf = {
        DATA_SOURCE_CSV: {TTJ: "TTJHP_JOB_aggregagateStatistics.csv",
                          HJ: "HASH_JOIN_JOB_aggregagateStatistics.csv",
                          Yannakakis1Pass: "Yannakakis1Pass_JOB_aggregagateStatistics.csv"}
    }
    agg_data = cleanup_csv(conf[DATA_SOURCE_CSV], get_job_full_path)
    input_size_ratio, intermediate_ratio, tuple_removed_ratio = dict(), dict(), dict()
    for algorithm in agg_data:
        hj_input = agg_data[HJ][input_size_after_evaluation]
        hj_intermediate = agg_data[HJ][total_intermediate_results_produced]
        input_size_ratio[algorithm] = \
            [algorithm_input_size / hj_input_size for hj_input_size, algorithm_input_size in
             zip(hj_input, agg_data[algorithm][input_size_after_evaluation])]
        intermediate_ratio[algorithm] = \
            [algorithm_intermediate_size / hj_intermediate_size for hj_intermediate_size, algorithm_intermediate_size in
             zip(hj_intermediate, agg_data[algorithm][total_intermediate_results_produced])]
        tuple_removed_ratio[algorithm] = [1 - input_size_ratio_individual for input_size_ratio_individual in
                                          input_size_ratio[algorithm]]
    del input_size_ratio[HJ]
    del intermediate_ratio[HJ]
    del tuple_removed_ratio[HJ]
    return intermediate_ratio, input_size_ratio, tuple_removed_ratio


def plot_ssb2(ratio, filename, ylabel):
    def bar_plot(ax, data, labels, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        patterns = ["\\", "-", "/", ""]

        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        n_bars = len(data)
        bar_width = total_width / n_bars
        bars = []
        for i, name in enumerate([TTJ, Yannakakis1Pass]):
            values = data[name]
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
            bars.append(bar[0])
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{YA}^+$'], frameon=False, fontsize=10, ncol=4,
                      loc='best')

        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict, rotation=270)
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
        ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=0))
        ax.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, axs = plt.subplots(figsize=(14, 3), dpi=140)
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

    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 8}
    bar_plot(axs, ratio, labels=labels, colors=[TTJ_COLOR, Yannakakis1Pass_COLOR], total_width=.7,
             single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 10}
    axs.set_ylabel(ylabel, fontdict=font2)
    plt.margins(x=0.01)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    intermediate_ratio, input_size_ratio, tuple_removed_ratio = processing()
    plot_ssb2(tuple_removed_ratio, "exp2.2.d.job.3.pdf", 'Fraction of input tuples removed')
