"""
Exp 2.9: Impact of Removing Dangling Tuples from R_{inner}
"""
import numpy as np
from matplotlib import transforms, ticker

"""
Exp 2.8: Impact of backjumping
"""
import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt

from plot.constants import DATA_SOURCE_CSV, TTJ_BF, TTJ_NO_NG
from plot.utility import check_argument


def get_benchmark_results_full_path(json_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "exp2p9" / json_name

def get_benchmark_results_full_path_o(json_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "exp2p9o" / json_name


def extract_data_from_json(DATA_SOURCE_JSON, get_full_path_func):
    benchmark_json_full_path = get_full_path_func(DATA_SOURCE_JSON)
    result = dict()
    with open(benchmark_json_full_path, "r") as read_file:
        json_result = json.load(read_file)
        for benchmark_dict in json_result:
            join_operator = benchmark_dict['params']['joinOperator']
            query = benchmark_dict['params']['exp2P9Queries']
            execution_time = benchmark_dict['primaryMetric']['score']
            if join_operator not in result:
                result[join_operator] = dict()
            result[join_operator][query] = execution_time
    return result


def extract_list(result_dict):
    def extract_ratio(query):
        y_index = query.index('y')
        p_indexes = [i for i, ltr in enumerate(query) if ltr == 'P']
        return query[y_index+1: p_indexes[-1]]

    queries = list(result_dict[TTJ_BF])
    sorted(queries, key=str.lower)
    ttj_bf_list = []
    ttj_no_ng_list = []
    semijoinmodratio_list = []
    for query in queries:
        ttj_bf_list.append(result_dict[TTJ_BF][query])
        ttj_no_ng_list.append(result_dict[TTJ_NO_NG][query])
        semijoinmodratio_list.append(int(extract_ratio(query)))
    return ttj_no_ng_list, ttj_bf_list, semijoinmodratio_list


def plot_impact_of_semijoinmodratio(benchmark_json, file_name, get_full_path_func):
    conf = {
        DATA_SOURCE_CSV: benchmark_json,
    }
    result_dict = extract_data_from_json(conf[DATA_SOURCE_CSV], get_full_path_func)
    ttj_no_ng_list, ttj_bf_list, semijoinmodratio_list = extract_list(result_dict)

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
            ax.legend(bars, [r'$\mathsf{TTJ}^{bj}$', r'$\mathsf{TTJ}^{bj+rm}$'], frameon=False, fontsize=20, ncol=3,
                      loc='best')

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
        ax.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)


    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)

    data_to_plot = dict()
    data_to_plot[TTJ_BF] = ttj_bf_list
    data_to_plot[TTJ_NO_NG] = ttj_no_ng_list

    font = {'family': 'Helvetica',
            'size': 10}
    bar_plot(ax, data_to_plot, labels=semijoinmodratio_list, colors=['#00994D', '#FF9933'], total_width=.7,
             single_width=1,
             fontdict=font,
             legend=True)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    ax.set_ylabel(r'Execution time (ms)', fontdict=font2)
    ax.set_xlabel(r'$\theta$', fontdict=font2, loc='center')
    plt.tight_layout()
    plt.savefig(file_name, format='pdf')
    plt.show()



    #
    # ax.plot(semijoinmodratio_list, ttj_bf_list, marker='o', color='#00994D', label=r'$\mathsf{TTJ}^{bj}$')
    # ax.plot(semijoinmodratio_list, ttj_no_ng_list, marker='*', color='#FF0000', label=r'$\mathsf{TTJ}^{bj+rm}$')
    # plt.grid(which='major', axis='y', zorder=-1.0)
    # font2 = {'family': 'Helvetica',
    #         'weight': 'bold',
    #         'size': 15}
    # plt.ylabel(r'Execution time (ms) in $\log_{10}$ scale', fontdict=font2)
    # plt.xlabel(r'$\theta$', fontdict=font2)
    # ax.set_axisbelow(True)
    # plt.tight_layout()
    # plt.legend(fontsize=20, loc='best')
    # filename = "exp2.9.pdf"
    # plt.savefig(filename, format='pdf')
    # plt.show()




if __name__ == "__main__":
    plot_impact_of_semijoinmodratio("benchmarkexp2p9-result-2024-01-12t21:27:42.365185.json", "exp2.9.pdf", get_benchmark_results_full_path)
    plot_impact_of_semijoinmodratio("benchmarkexp2p9o-result-2024-01-13t15:42:33.500649.json", "exp2.9.o.pdf", get_benchmark_results_full_path_o)