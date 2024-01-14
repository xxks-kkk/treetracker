"""
Exp 2.8: Impact of backjumping
"""
import json
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt

from plot.constants import DATA_SOURCE_CSV
from plot.utility import check_argument


def get_benchmark_results_full_path(json_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "exp2p8" / json_name


def extract_data_from_json(DATA_SOURCE_JSON, get_full_path_func):
    benchmark_json_full_path = get_full_path_func(DATA_SOURCE_JSON)
    result = dict()
    with open(benchmark_json_full_path, "r") as read_file:
        json_result = json.load(read_file)
        for benchmark_dict in json_result:
            query = benchmark_dict['params']['exp2p8Queries']
            execution_time = benchmark_dict['primaryMetric']['score']
            result[query] = execution_time
    return result

def get_join_tree_and_k(query):
    removed_exp_prefix = query[6:]
    N_index = removed_exp_prefix.index('N')
    target_str = removed_exp_prefix[:N_index]
    parts = target_str.split('Query')
    return parts[0], int(parts[1][1:])

def plot_impact_of_backjump_scale_with_number_of_backjump_relations():
    conf = {
        DATA_SOURCE_CSV: "benchmarkexp2p8-result-2023-12-05t22:31:15.037661.json",
    }
    result_dict = extract_data_from_json(conf[DATA_SOURCE_CSV], get_benchmark_results_full_path)

    join_tree_1 = []
    join_tree_2 = []
    scale_factors = []

    for query in result_dict:
        join_tree, k = get_join_tree_and_k(query)
        # if len(scale_factors) != 0:
        #     check_argument(scale_factors[-1] == k+2,
        #                    "we assume the keys in result_dict are ordered: for a fixed k, JoinTree1 result and JoinTree2 result are grouped next to each other")
        if len(scale_factors) == 0 or scale_factors[-1] != k+2:
            scale_factors.append(k+2)
        if join_tree == 'JoinTree1':
            join_tree_1.append(result_dict[query])
        elif join_tree == 'JoinTree2':
            join_tree_2.append(result_dict[query])


    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)
    # ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=10)
    ax.minorticks_off()
    ax.plot(scale_factors, join_tree_1, marker='o', color='#00994D', label=r'$\mathsf{TTJ}^{bj^-}$')
    ax.plot(scale_factors, join_tree_2, marker='*', color='#FF0000', label=r'$\mathsf{TTJ}^{bj^+}$')
    plt.grid(which='major', axis='y', zorder=-1.0)
    font2 = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    plt.ylabel(r'Execution time (ms) in $\log_{10}$ scale', fontdict=font2)
    plt.xlabel(r'$k$', fontdict=font2)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    ax.set_axisbelow(True)
    plt.tight_layout()
    plt.legend(fontsize=20, loc='best')
    filename = "exp2.8.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


def additional_num_of_dangling_tuples(n, k):
    """
    :param n: input relation size for backjumped relations
    :param k: number of input relations (number of backjumped relaiton is k-2)
    :return:
    """
    sum = 0
    for j in range(k-2):
        sum += (n-2) * pow(n-1, j)
    return sum




if __name__ == "__main__":
    plot_impact_of_backjump_scale_with_number_of_backjump_relations()
    check_argument(additional_num_of_dangling_tuples(4,5) == 26, "compute additional number of dangling tuples incorrectly")
    print(additional_num_of_dangling_tuples(10,6))