import json
import os
import string
import sys
from pathlib import Path
from typing import Tuple, Dict, Union, List, Any

import matplotlib.pyplot as plt
import numpy as np

"""
Draw execution time for different queries within benchmark

Usage:
  drawExecutionTimeForDifferentQueriesWithinBenchmark [path_to_json_data]

Ref:
- https://matplotlib.org/stable/gallery/lines_bars_and_markers/barchart.html
"""

# indicates all possible queries we can benchmark. Note that those keywords
# are tightly coupled with corresponding java method names in the benchmark.
QUERY_KEYWORDS = {'queryOneOne': 'Query 1', 'queryTwo': 'Query 2', 'queryThree': 'Query 3', 'queryFour': 'Query 4'}
# indicates all possible operators we can benchmark. Similar to QUERY_KEYWORDS,
# those keywords are tightly coupled with corresponding java method names in the benchmark.
OPERATOR_KEYWORDS = {'HashJoin': 'Hash Join', 'TTJ': 'TTJ', 'LIP': 'LIP'}


def main(argv):
    filepath = argv[0]
    data, json_file_name = parse_json_data(filepath)
    draw(data, json_file_name)


def draw(data: dict, json_file_name: string) -> None:
    print(data)
    labels = data['labels']

    x = np.arange(len(labels))  # the label locations
    width = 0.2 # the width of the bars

    rects = []
    fig, ax = plt.subplots()
    number_of_dp_series = len(data['datapoints'])
    x_rects = []  # the list of rects position
    # TODO: modify here if there are more operators (currently, we have 3)
    if number_of_dp_series == 3:
        x_rects.append(x - width)
        x_rects.append(x)
        x_rects.append(x + width)

    for kv_pair, x_rect in zip(data['datapoints'].items(), x_rects):
        rects.append(ax.bar(x_rect, kv_pair[1], width, label=kv_pair[0]))

    # # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Execution time (seconds)')
    ax.set_title(data['figure_title'])
    ax.set_xticks(x, labels)
    ax.legend()

    for rect in rects:
        ax.bar_label(rect, padding=3)

    fig.tight_layout()
    plt.savefig(os.path.join(data['data_dir'], 'drawExecutionTimeForDifferentQueriesWithinSSB-' + json_file_name + '.pdf'), format='pdf')
    plt.show()


def parse_json_data(filepath: string) -> Tuple[Dict[str, Union[str, List[Any], Dict[Any, Any]]], str]:
    ret_data = {}
    json_file_name = Path(filepath).stem
    with open(filepath) as json_file:
        json_data = json.load(json_file)
        ret_data['data_dir'] = str(Path(filepath).parent)
        ret_data['figure_title'] = generate_figure_title(json_data)
        ret_data['labels'] = []
        ret_data['datapoints'] = {}
        for benchmarkRes in json_data:
            benchmarkField = benchmarkRes['benchmark']
            if benchmarkRes['primaryMetric']['scoreUnit'] != 'ms/op':
                raise Exception('json file score unit is not ms/op')
            for query_keyword in QUERY_KEYWORDS.keys():
                if query_keyword in benchmarkField and QUERY_KEYWORDS[query_keyword] not in ret_data['labels']:
                    ret_data['labels'].append(QUERY_KEYWORDS[query_keyword])
                    break
            cur_operator_label = ''
            for operator_keyword in OPERATOR_KEYWORDS.keys():
                if operator_keyword in benchmarkField:
                    cur_operator_label = OPERATOR_KEYWORDS[operator_keyword]
                    if OPERATOR_KEYWORDS[operator_keyword] not in ret_data['datapoints']:
                        ret_data['datapoints'][cur_operator_label] = []
                        break
            ret_data['datapoints'][cur_operator_label].append(round(benchmarkRes['primaryMetric']['score'] / 1000, 3))
        return ret_data, json_file_name


def generate_figure_title(json_data: json):
    benchmark_str = json_data[0]['benchmark']
    if 'ssb' in benchmark_str:
        return 'Star Schema Benchmark Queries'


def range_inc(start, num_times, step):
    ret = []
    i = start
    j = 0
    while j < num_times:
        ret.append(i)
        i += step
        j += 1
    return ret


if __name__ == '__main__':
    main(sys.argv[1:])
