"""
Exp1.3: We compare join time vs. preprocessing time across all algorithms
"""
import csv
import re
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import DATA_SOURCE_CSV, TTJ, HJ, Yannakakis, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, \
    YannakakisB, TTJ_REDUC_TIME, HJ_REDUC_TIME, Yannakakis_REDUC_TIME, YannakakisB_REDUC_TIME, TTJ_JOIN_TIME, \
    HJ_JOIN_TIME, Yannakakis_JOIN_TIME, YannakakisB_JOIN_TIME, Yannakakis1Pass, Yannakakis1Pass_REDUC_TIME, \
    Yannakakis1Pass_JOIN_TIME, TTJ_REDUC_TIME_COLOR, TTJ_JOIN_TIME_COLOR, Yannakakis1Pass_REDUC_TIME_COLOR, \
    Yannakakis1Pass_JOIN_TIME_COLOR
from plot.job import extract_data_from_csv
from plot.utility import to_float, check_argument

join_fragment_eval_count = "join_fragment_eval_count"
remove_dangling_tuple_from_rinner_count = "remove_dangling_tuple_from_rinner_count"
full_reducer_count = "full_reducer_count"
cursor_count = "cursor_count"
ng_probing_count = "ng_probing_count"
ng_construct_count = "ng_construct_count"


def get_profiling_results_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "profiling" / csv_name


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


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
                if join_fragment_eval_count in row:
                    result[algorithm][join_fragment_eval_count] = [to_float(num) for num in row[1:]]
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

def extract_data_based_on_labels(prem_data, labels):
    job_labels = ["1a", "1b", "1c", "1d",
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
    return_data = dict()
    for algorithm, dps in prem_data.items():
        return_data[algorithm] = []
        for label in labels:
            return_data[algorithm].append(dps[job_labels.index(label)])
    return return_data

def plot_job():
    conf = {
        DATA_SOURCE_CSV: {TTJ: "job_TTJHP_profiling_agg.csv",
                          Yannakakis1Pass: "job_Yannakakis1Pass_profiling_agg.csv"},
    }
    agg_data = cleanup_csv(conf[DATA_SOURCE_CSV], get_profiling_results_full_path)

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
                reduction_time_percentage[algorithm].append(
                    (reduction_time[algorithm][i] - agg_data[algorithm][cursor_count][i]) / (
                                agg_data[algorithm][join_fragment_eval_count][i] - agg_data[algorithm][cursor_count][
                            i]))
            else:
                reduction_time_percentage[algorithm].append(
                    reduction_time[algorithm][i] / agg_data[algorithm][join_fragment_eval_count][i])
    reduction_time_percentage[HJ] = [0] * len(reduction_time_percentage[TTJ])

    job_plot_with_predicates = {
        DATA_SOURCE_CSV: "benchmarkjobwithpredicatesfixedhjorderingshallow-result-2024-07-07t17:19:27.127911_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis1Pass],
        COLUMN_RIGHT_BOUND: 114,
    }
    prem_data,_ = extract_data_from_csv(get_job_full_path(job_plot_with_predicates[DATA_SOURCE_CSV]),
                                         column_range=[1, job_plot_with_predicates[COLUMN_RIGHT_BOUND]])
    labels = ["6a", "6b", "6c", "6d", "6e", "7b", "11b", "12b", "17a", "18a", "32a"]


    # check_argument(len(prem_data.keys()) >= len(job_plot_with_predicates[ALGORITHMS_TO_PLOT]),
    #                f"there should be {len(job_plot_with_predicates[ALGORITHMS_TO_PLOT])} algorithms in data. Current only have {prem_data.keys()}")
    # for dps in prem_data.values():
    #     check_argument(len(dps) == len(labels),
    #                    f"some query data is missing. There should be {len(labels)} dps. Instead, we have {len(dps)}")
    prem_data = extract_data_based_on_labels(prem_data, labels)

    data_normalized = dict()
    for algorithm, dps in prem_data.items():
        hj = prem_data[HJ]
        data_normalized[algorithm] = [algorithm_time / hj_time * 100 for hj_time, algorithm_time in zip(hj, prem_data[algorithm])]
    prem_data = data_normalized

    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in job_plot_with_predicates[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    patterns = ["\\", "-", "/", "", ".", '+']

    def bar_plot2(ax, data, labels, colors=None, total_width=0.8, single_width=1,
                  legend=True, fontdict=None):
        patterns = ["", "\\", "", "-",  "", "/"]
        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']
        n_bars = len(data)
        bar_width = total_width / n_bars
        bars = []
        for i, name in enumerate([TTJ_REDUC_TIME, TTJ_JOIN_TIME, Yannakakis1Pass_REDUC_TIME, Yannakakis1Pass_JOIN_TIME]):
            values = data[name]
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2
            idx = 0
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
                # ax.bar_label(bar, ["{:.0%}".format(reduction_time_percentage_use[idx])], label_type='center')
                idx += 1
            bars.append(bar[0])
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}^R$', r'$\mathsf{TTJ}^\bowtie$',
                             r'${\mathsf{YA}^+}^R$', r'${\mathsf{YA}^+}^\bowtie$'], frameon=False, fontsize=10, ncol=4,
                      loc='best')
        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        ax.axhline(y=100, color='#FF0000', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 10}
        ax.text(0, 103, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        ax.grid(which='major', axis='y', zorder=-1.0)
        # ax.set_ylim(ymax=100)
        ax.set_axisbelow(True)
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(1, 1, figsize=(6, 6), dpi=140)

    font = {'family': 'Helvetica',
            'size': 10}
    # bar_plot(ax, data, labels=labels, colors=["#FF0000", '#00994D', '#FF9933', "#00C3E3"], total_width=.7,
    #          single_width=1,
    #          fontdict=font,
    #          legend=False)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 15}
    # ax.set_ylabel('execution time (ms)', fontdict=font2)
    ax.set_ylabel('Normalized execution time', fontdict=font2)


    preprocessing_time = dict()
    for algorithm in data:
        preprocessing_time[algorithm] = []
        for execution_time, reduction_time_precent in zip(data[algorithm], reduction_time_percentage[algorithm]):
            preprocessing_time[algorithm].append(execution_time * reduction_time_precent)
    join_time = dict()
    for algorithm in data:
        join_time[algorithm] = []
        for execution_time, preprocessing_t in zip(data[algorithm], preprocessing_time[algorithm]):
            join_time[algorithm].append(execution_time - preprocessing_t)

    summary = dict()
    for algorithm in preprocessing_time:
        if algorithm == TTJ:
            summary[TTJ_REDUC_TIME] = preprocessing_time[algorithm]
        # elif algorithm == HJ:
        #     summary[HJ_REDUC_TIME] = preprocessing_time[algorithm]
        elif algorithm == Yannakakis1Pass:
            summary[Yannakakis1Pass_REDUC_TIME] = preprocessing_time[algorithm]
    for algorithm in join_time:
        if algorithm == TTJ:
            summary[TTJ_JOIN_TIME] = join_time[algorithm]
        elif algorithm == HJ:
            summary[HJ_JOIN_TIME] = join_time[algorithm]
        elif algorithm == Yannakakis1Pass:
            summary[Yannakakis1Pass_JOIN_TIME] = join_time[algorithm]


    bar_plot2(ax, summary, labels,
              colors=[TTJ_REDUC_TIME_COLOR, TTJ_JOIN_TIME_COLOR, Yannakakis1Pass_REDUC_TIME_COLOR, Yannakakis1Pass_JOIN_TIME_COLOR], total_width=.7,
              single_width=1,
              fontdict=font,
              legend=True)

    plt.tight_layout()
    filename = "exp1.3.e.3.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    plot_job()
