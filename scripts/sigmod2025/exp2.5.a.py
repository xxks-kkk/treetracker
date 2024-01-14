"""
Exp 2.5: reduction time % for Impact of ng at R_k

Bar plot with execution time
"""

import json
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import DATA_SOURCE_CSV, TTJ


def get_benchmark_results_full_path(json_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "exp2p5" / json_name

def extract_data_from_agg_csv(DATA_SOURCE_CSV, get_full_path_func):
    benchmark_json_full_path = get_full_path_func(DATA_SOURCE_CSV)
    result = dict()
    with open(benchmark_json_full_path, "r") as read_file:
        json_result = json.load(read_file)
        for benchmark_dict in json_result:
            query = benchmark_dict['params']['exp2P5Queries']
            execution_time = benchmark_dict['primaryMetric']['score']
            result[query] = execution_time
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
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'Yannakakis', r'$\mathsf{LIP}$'], frameon=False, fontsize=10, ncol=3,
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
        ax.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    conf = {
        DATA_SOURCE_CSV: "benchmarkexp2p5-result-2023-11-26t21:43:59.103228.json",
    }
    result_dict = extract_data_from_agg_csv(conf[DATA_SOURCE_CSV], get_benchmark_results_full_path)
    queries = result_dict.keys()

    labels = ['Exp2P5Query0P', 'Exp2P5Query10P', 'Exp2P5Query20P', 'Exp2P5Query30P', 'Exp2P5Query40P',
              'Exp2P5Query50P', 'Exp2P5Query60P', 'Exp2P5Query70P', 'Exp2P5Query80P', 'Exp2P5Query90P',
              'Exp2P5Query100P']
    execution_time = []
    for i in range(len(labels)):
        execution_time.append(result_dict[labels[i]])

    data_to_plot = dict()
    data_to_plot[TTJ] = execution_time
    labels_to_use = ['0%', '10%', '20%', '30%', '40%', '50%', '60%', '70%', '80%', '90%', '100%']

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(1, 1, figsize=(6, 6), dpi=140)

    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(ax, data_to_plot, labels=labels_to_use, colors=['#00994D', '#FF9933', '#9933FF'], total_width=.7,
             single_width=1,
             fontdict=font,
             legend=False)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    ax.set_ylabel('execution time (ms)', fontdict=font2)
    ax.set_xlabel(r'$\alpha$', fontdict=font2, loc='center')
    plt.tight_layout()
    filename = "exp2.5.a.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    plot_ssb()
