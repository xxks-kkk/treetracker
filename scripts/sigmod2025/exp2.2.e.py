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

from plot.constants import DATA_SOURCE_CSV, HJ, TTJ, Yannakakis, LIP, YannakakisB, HJ_TTJ_ORDER, HJ_YA_ORDER
from plot.utility import to_float

input_size_after_evaluation = "input_size_after_evaluation"
total_intermediate_results_produced = "total_intermediate_results_produced"

def get_tpch_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "simple-cost-model-with-predicates" / csv_name


def extract_number_from_str(s):
    return re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s)

def cleanup_csv(DATA_SOURCE_CSV, get_full_path_func):
    """
    For TPC-H, we combine
    - 7a with 7b
    - 19a, 19b, 19c together
    """
    full_data_source_csv = dict()
    for algorithm, csvfile in DATA_SOURCE_CSV.items():
        full_data_source_csv[algorithm] = get_full_path_func(csvfile)
    result = dict()
    for algorithm, csv_file_path in full_data_source_csv.items():
        result[algorithm] = dict()
        with open(csv_file_path, "r") as file:
            csv_file = csv.reader(file)
            headers = next(csv_file)
            headers_tmp = []
            rows_tmp = []
            for header in headers:
                if header == 'statistics':
                    headers_tmp.append(header)
                else:
                    headers_tmp.append(int(extract_number_from_str(header)[0]))
            for row in csv_file:
                rows_tmp.append(row)
            seven_idx = []
            nineteen_idx = []
            for i in range(len(headers_tmp)):
                if headers_tmp[i] == 7:
                    seven_idx.append(i)
                elif headers_tmp[i] == 19:
                    nineteen_idx.append(i)
            rows_use = []
            for row in rows_tmp:
                rows_use.append([])
                for i in range(len(headers_tmp)):
                    if i == seven_idx[0]:
                        val = 0
                        for idx in seven_idx:
                            val += int(row[idx])
                        rows_use[-1].append(val)
                    elif i == nineteen_idx[0]:
                        val = 0
                        for idx in nineteen_idx:
                            val += int(row[idx])
                        rows_use[-1].append(val)
                    elif i in seven_idx or i in nineteen_idx:
                        pass
                    elif i == 0:
                        rows_use[-1].append(row[i])
                    else:
                        rows_use[-1].append(int(row[i]))
            headers_use = []
            for i in range(len(headers_tmp)):
                if i == seven_idx[0]:
                    headers_use.append(7)
                elif i == nineteen_idx[0]:
                    headers_use.append(19)
                elif i in seven_idx or i in nineteen_idx:
                    pass
                else:
                    headers_use.append(headers_tmp[i])
            # print(rows_use)
            # print(headers_use)
            headers_no_fileds = headers_use[1:]
            idx = np.argsort(headers_no_fileds)
            headers_sorted = [headers_use[0]]
            for i in idx:
                headers_sorted.append(headers_no_fileds[i])
            rows_sorted = []
            for row in rows_use:
                rows_sorted.append([row[0]])
                row_no_fields = row[1:]
                for i in idx:
                    rows_sorted[-1].append(row_no_fields[i])
            # print(rows_sorted)
            # print(headers_sorted)
            for row in rows_sorted:
                if "totalIntermediateResultsProduced" in row:
                    result[algorithm][total_intermediate_results_produced] = [to_float(num) for num in row[1:]]
                elif "totalInputSizeAfterEvaluation" in row:
                    result[algorithm][input_size_after_evaluation] = [to_float(num) for num in row[1:]]
    return result



def processing():
    conf = {
        DATA_SOURCE_CSV: {TTJ: "TTJHP_TPCH_aggregagateStatistics.csv",
                          HJ_TTJ_ORDER: "HASH_JOIN_TPCH_hashJoinOnTTJOrdering_aggregagateStatistics.csv",
                          HJ_YA_ORDER: "HASH_JOIN_TPCH_hashJoinOnYAOrdering_aggregagateStatistics.csv",
                          Yannakakis : "Yannakakis_TPCH_aggregagateStatistics.csv",
                          YannakakisB: "YannakakisB_TPCH_aggregagateStatistics.csv"}
    }
    agg_data = cleanup_csv(conf[DATA_SOURCE_CSV], get_tpch_full_path)
    input_size_ratio, intermediate_ratio, tuple_removed_ratio = dict(), dict(), dict()
    for algorithm in agg_data:
        if algorithm == TTJ:
            hj_input = agg_data[HJ_TTJ_ORDER][input_size_after_evaluation]
            hj_intermediate = agg_data[HJ_TTJ_ORDER][total_intermediate_results_produced]
            input_size_ratio[algorithm] = \
                [algorithm_input_size / hj_input_size for hj_input_size, algorithm_input_size in zip(hj_input, agg_data[algorithm][input_size_after_evaluation])]
            intermediate_ratio[algorithm] = \
                [algorithm_intermediate_size / hj_intermediate_size for hj_intermediate_size, algorithm_intermediate_size in
                 zip(hj_intermediate, agg_data[algorithm][total_intermediate_results_produced])]
            tuple_removed_ratio[algorithm] = [1 - input_size_ratio_individual for input_size_ratio_individual in input_size_ratio[algorithm]]
        elif algorithm == YannakakisB or algorithm == Yannakakis:
            hj_input = agg_data[HJ_YA_ORDER][input_size_after_evaluation]
            hj_intermediate = agg_data[HJ_YA_ORDER][total_intermediate_results_produced]
            input_size_ratio[algorithm] = \
                [algorithm_input_size / hj_input_size for hj_input_size, algorithm_input_size in zip(hj_input, agg_data[algorithm][input_size_after_evaluation])]
            intermediate_ratio[algorithm] = \
                [algorithm_intermediate_size / hj_intermediate_size for hj_intermediate_size, algorithm_intermediate_size in
                 zip(hj_intermediate, agg_data[algorithm][total_intermediate_results_produced])]
            tuple_removed_ratio[algorithm] = [1 - input_size_ratio_individual for input_size_ratio_individual in input_size_ratio[algorithm]]
    return intermediate_ratio, input_size_ratio, tuple_removed_ratio

def ignore_dp(ratio_list, labels, ignore_labels):
    ignore_idx = [ignore_labels.index(ignore_label) for ignore_label in ignore_labels]
    for idx in ignore_idx:
        for ratio in ratio_list:
            for algorithm in ratio:
                del ratio[algorithm][idx]



def plot_ssb(ratio, filename, ylabel):
    def bar_plot(ax, data, labels, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        patterns = ["\\", "-", "/", ""]

        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        n_bars = len(data)
        bar_width = total_width / n_bars
        bars = []
        for i, name in enumerate([TTJ, Yannakakis, YannakakisB]):
            values = data[name]
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
            bars.append(bar[0])
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{YA}$', r'$\mathsf{PT}$'], frameon=False, fontsize=15, ncol=3,
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
    labels = ["3", "7", "8", "9", "10", "11", "12", "14", "15", "16", "18", "19", "20"]


    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(axs, ratio, labels=labels, colors=['#00994D', '#FF9933', "#00C3E3"], total_width=.7,
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
        for i, name in enumerate([TTJ, Yannakakis, YannakakisB]):
            values = data[name]
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
            bars.append(bar[0])
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{YA}$', r'$\mathsf{PT}$'], frameon=False, fontsize=10, ncol=4,
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
    labels = ["3", "7", "8", "9", "10", "11", "12", "14", "15", "16", "18", "19", "20"]


    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(axs, ratio, labels=labels, colors=['#00994D', '#FF9933', "#00C3E3"], total_width=.7,
             single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    axs.set_ylabel(ylabel, fontdict=font2)
    plt.tight_layout()
    plt.savefig(filename, format='pdf')
    plt.show()



if __name__ == "__main__":
    intermediate_ratio, input_size_ratio, tuple_removed_ratio = processing()
    # ignore_dp([intermediate_ratio, input_size_ratio, tuple_removed_ratio], ["3", "7", "8", "9", "10", "11", "12", "14", "15", "16", "18", "19", "20"], ["18"])
    plot_ssb(intermediate_ratio, "exp2.2.e.1.pdf", r'Fraction of $\mathsf{HJ}$ (use each algorithm order) intermediate result size')
    plot_ssb(input_size_ratio, "exp2.2.e.2.pdf", r'Fraction of input tuples remained')
    plot_ssb2(tuple_removed_ratio, "exp2.2.e.3.pdf", 'Fraction of input tuples removed')