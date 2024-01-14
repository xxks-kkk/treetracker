"""
Experiment: Total intermediate results produced

The scatter plot with x-axis: input size after evaluation; y-axis: intermediate result size produced.
Each query has a scatter subplot.
"""
import csv
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import ticker, transforms

from plot.constants import DATA_SOURCE_CSV, HJ, TTJ, Yannakakis, LIP, YannakakisB
from plot.utility import to_float

input_size_after_evaluation = "input_size_after_evaluation"
total_intermediate_results_produced = "total_intermediate_results_produced"

def get_ssb_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "simple-cost-model-with-predicates" / csv_name


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
                if "totalIntermediateResultsProduced" in row:
                    result[algorithm][total_intermediate_results_produced] = [to_float(num) for num in row[1:]]
                elif "totalInputSizeAfterEvaluation" in row:
                    result[algorithm][input_size_after_evaluation] = [to_float(num) for num in row[1:]]
    return result


def processing():
    conf = {
        DATA_SOURCE_CSV: {TTJ: "TTJHP_SSB_aggregagateStatistics.csv",
                          HJ: "HASH_JOIN_SSB_aggregagateStatistics.csv",
                          Yannakakis : "Yannakakis_SSB_aggregagateStatistics.csv",
                          LIP : "LIP_SSB_aggregagateStatistics.csv",
                          YannakakisB: "YannakakisB_SSB_aggregagateStatistics.csv"}
    }
    prem_data = extract_data_from_agg_csv(conf[DATA_SOURCE_CSV], get_ssb_full_path)

    input_size_ratio, intermediate_ratio, tuple_removed_ratio = dict(), dict(), dict()
    for algorithm in prem_data:
        hj_input = prem_data[HJ][input_size_after_evaluation]
        hj_intermediate = prem_data[HJ][total_intermediate_results_produced]
        input_size_ratio[algorithm] = \
            [algorithm_input_size / hj_input_size for hj_input_size, algorithm_input_size in zip(hj_input, prem_data[algorithm][input_size_after_evaluation])]
        intermediate_ratio[algorithm] = \
            [algorithm_intermediate_size / hj_intermediate_size for hj_intermediate_size, algorithm_intermediate_size in
             zip(hj_intermediate, prem_data[algorithm][total_intermediate_results_produced])]
        tuple_removed_ratio[algorithm] = [1 - input_size_ratio_individual for input_size_ratio_individual in input_size_ratio[algorithm]]
    del input_size_ratio[HJ]
    del intermediate_ratio[HJ]
    del tuple_removed_ratio[HJ]
    return intermediate_ratio, input_size_ratio, tuple_removed_ratio

def plot_ssb(ratio, filename, ylabel):
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
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{YA}$', r'$\mathsf{LIP}$', r'$\mathsf{PT}$'], frameon=False, fontsize=15, ncol=2,
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
        ax.yaxis.set_major_formatter(ticker.PercentFormatter(1, decimals=0))
        ax.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)



    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, axs = plt.subplots(1, 1, figsize=(6, 6), dpi=140)
    labels = ["1.1", "1.2", "1.3", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3"]


    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(axs, ratio, labels=labels, colors=['#00994D', '#FF9933', '#9933FF',  "#00C3E3"], total_width=.7,
             single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    axs.set_ylabel(ylabel, fontdict=font2)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()


def plot_ssb2(ratio, filename, ylabel):
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
    fig, axs = plt.subplots(1, 1, figsize=(6, 6), dpi=140)
    labels = ["1.1", "1.2", "1.3", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3"]


    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(axs, ratio, labels=labels, colors=['#00994D', '#FF9933', '#9933FF',  "#00C3E3"], total_width=.7,
             single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    axs.set_ylabel(ylabel, fontdict=font2)
    axs.set_ylim(ymin=0.82, ymax=1.015)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()

if __name__ == "__main__":
    intermediate_ratio, input_size_ratio, tuple_removed_ratio = processing()
    plot_ssb(intermediate_ratio, "exp2.2.c.1.pdf", r'Fraction of $\mathsf{HJ}$ intermediate result size')
    plot_ssb(input_size_ratio, "exp2.2.c.2.pdf", r'Fraction of input tuples remained')
    plot_ssb2(tuple_removed_ratio, "exp2.2.c.3.pdf", 'Fraction of input tuples removed')