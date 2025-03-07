"""
Removing tuple overhead from TTJ
"""
import csv
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import TTJ, DATA_SOURCE_CSV, ALGORITHMS_TO_PLOT, COLUMN_RIGHT_BOUND, TTJ_COLOR
from plot.cost_model4 import extract_data_from_csv
from plot.utility import to_float, check_argument

join_fragment_eval_count = "join_fragment_eval_count"
remove_dangling_tuple_from_rinner_count = "remove_dangling_tuple_from_rinner_count"

def get_profiling_results_full_path():
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "profiling" / "job_TTJHP_profiling_agg.csv"

def extract_data_from_agg_csv():
    csv_full_path = get_profiling_results_full_path()
    result = dict()
    with open(csv_full_path, "r") as file:
        csv_file = csv.reader(file)
        headers = next(csv_file)
        for row in csv_file:
            if join_fragment_eval_count in row:
                result[join_fragment_eval_count] = [to_float(num) for num in row[1:]]
            elif remove_dangling_tuple_from_rinner_count in row:
                result[remove_dangling_tuple_from_rinner_count] = [to_float(num) for num in row[1:]]
    return headers, result

def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


def plot_job():
    def bar_plot(ax, data, labels, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        n_bars = len(data)
        bar_width = total_width / n_bars
        bars = []
        for i, name in enumerate([TTJ]):
            values = data[name]
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)])
            bars.append(bar[0])
        if legend:
            ax.legend(bars, [r'deleteDT() overhead'], fontsize=13, ncol=4, frameon=False,
                      loc='center', bbox_to_anchor=(0.70, 1.25))

        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict, rotation=30)
        else:
            ax.set_xticks(x, labels, rotation=270)
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 9}
        ax.minorticks_off()
        ax.set_yscale('log')
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        ax.yaxis.set_major_formatter(ticker.PercentFormatter(1.0))
        ax.grid(which='major', axis='y', zorder=-1.0)
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

    job_plot = {
        DATA_SOURCE_CSV: "2024-11-03T23:22:33.790065Z_perf_report.csv",
        ALGORITHMS_TO_PLOT: [TTJ],
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])

    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in job_plot[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    for dps in data.values():
        check_argument(len(dps) == job_plot[COLUMN_RIGHT_BOUND] - 1,
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

    headers,results = extract_data_from_agg_csv()
    def process_header(header):
        return "query" + header[5:header.find('OptJoinTreeOptOrderingShallowHJOrdering')]

    data_speedup = dict()
    data_speedup[TTJ] = dict()
    headers_updated = list(map(lambda header : process_header(header), headers[1:]))
    check_argument(len(headers_updated) == job_plot[COLUMN_RIGHT_BOUND] - 1, "not enough data")
    for index, header in enumerate(headers_updated):
        runtime = prem_data[TTJ][header]
        join_fragment_eval_query = results[join_fragment_eval_count][index]
        overhead = results[remove_dangling_tuple_from_rinner_count][index]
        data_speedup[TTJ][header] = overhead / join_fragment_eval_query
    data_speedup = perform_grouping(data_speedup)

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, axs = plt.subplots(3, 1, figsize=(14, 4), dpi=140)

    numdp = len(labels)
    chunk_1_endpoint = round(numdp/3)
    chunk_2_endpoint = chunk_1_endpoint + round(numdp/3)
    data_chunk1, data_chunk2, data_chunk3 = dict(), dict(), dict()
    data_chunk1_label, data_chunk2_label, data_chunk3_label = labels[:chunk_1_endpoint],labels[chunk_1_endpoint:chunk_2_endpoint],labels[chunk_2_endpoint:]
    for algorithms, dps in data_speedup.items():
        data_chunk1[algorithms] = dps[:chunk_1_endpoint]
        data_chunk2[algorithms] = dps[chunk_1_endpoint:chunk_2_endpoint]
        data_chunk3[algorithms] = dps[chunk_2_endpoint:]

    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 10}
    bar_plot(axs[0], data_chunk1, data_chunk1_label, colors=[TTJ_COLOR], total_width=.7, single_width=1,
             fontdict=font)
    bar_plot(axs[1], data_chunk2, data_chunk2_label, colors=[TTJ_COLOR], total_width=.7, single_width=1,
             fontdict=font, legend=False)
    bar_plot(axs[2], data_chunk3, data_chunk3_label, colors=[TTJ_COLOR], total_width=.7, single_width=1,
             fontdict=font, legend=False)
    for i, ax in enumerate(axs):
        ax.margins(x=0.01)
        font2 = {'family': 'Helvetica',
                 'weight': 'bold',
                 'size': 13}
        if i == 1:
            ax.set_ylabel('Percentage of execution time (log scale)', fontdict=font2)
    plt.tight_layout()
    fig.align_ylabels(axs)
    filename = "overhead_plot.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()

if __name__ == "__main__":
    plot_job()