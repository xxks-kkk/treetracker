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
    HJ_JOIN_TIME, Yannakakis_JOIN_TIME, YannakakisB_JOIN_TIME
from plot.job import extract_data_from_csv
from plot.utility import to_float, check_argument

join_fragment_eval_count = "join_fragment_eval_count"
remove_dangling_tuple_from_rinner_count = "remove_dangling_tuple_from_rinner_count"
full_reducer_count = "full_reducer_count"
cursor_count = "cursor_count"
ng_probing_count = "ng_probing_count"
ng_construct_count = "ng_construct_count"


def get_profiling_results_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "others" / "profiling" / csv_name


def get_tpch_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2-local" / "results" / "tpch" / "with_predicates" / csv_name


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
                if header == 'stats':
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


def plot_tpch():
    conf = {
        DATA_SOURCE_CSV: {TTJ: "tpch_TTJHP_profiling_agg.csv",
                          Yannakakis: "tpch_Yannakakis_profiling_agg.csv",
                          YannakakisB: "tpch_YannakakisB_profiling_agg.csv"},
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

    tpch_plot_with_predicates = {
        DATA_SOURCE_CSV: "2024-01-12T05:55:47.613646Z_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis, YannakakisB],
        COLUMN_RIGHT_BOUND: 14,
    }
    prem_data, _ = extract_data_from_csv(get_tpch_full_path(tpch_plot_with_predicates[DATA_SOURCE_CSV]),
                                         column_range=[1, tpch_plot_with_predicates[COLUMN_RIGHT_BOUND]])
    labels = ["3", "7", "8", "9", "10", "11", "12", "14", "15", "16", "18", "19", "20"]

    check_argument(len(prem_data.keys()) >= len(tpch_plot_with_predicates[ALGORITHMS_TO_PLOT]),
                   f"there should be {len(tpch_plot_with_predicates[ALGORITHMS_TO_PLOT])} algorithms in data. Current only have {prem_data.keys()}")
    for dps in prem_data.values():
        check_argument(len(dps) == len(labels),
                       f"some query data is missing. There should be {len(labels)} dps. Instead, we have {len(dps)}")


    data_normalized = dict()
    for algorithm, dps in prem_data.items():
        hj = prem_data[HJ]
        data_normalized[algorithm] = [algorithm_time / hj_time * 100 for hj_time, algorithm_time in zip(hj, prem_data[algorithm])]
    prem_data = data_normalized

    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in tpch_plot_with_predicates[ALGORITHMS_TO_PLOT]:
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
        for i, name in enumerate([TTJ_REDUC_TIME, TTJ_JOIN_TIME, Yannakakis_REDUC_TIME, Yannakakis_JOIN_TIME, YannakakisB_REDUC_TIME, YannakakisB_JOIN_TIME]):
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
                             r'$\mathsf{YA}^R$', r'$\mathsf{YA}^\bowtie$',
                             r'$\mathsf{PT}^R$', r'$\mathsf{PT}^\bowtie$'], frameon=False, fontsize=10, ncol=2,
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
        elif algorithm == Yannakakis:
            summary[Yannakakis_REDUC_TIME] = preprocessing_time[algorithm]
        elif algorithm == YannakakisB:
            summary[YannakakisB_REDUC_TIME] = preprocessing_time[algorithm]
    for algorithm in join_time:
        if algorithm == TTJ:
            summary[TTJ_JOIN_TIME] = join_time[algorithm]
        elif algorithm == HJ:
            summary[HJ_JOIN_TIME] = join_time[algorithm]
        elif algorithm == Yannakakis:
            summary[Yannakakis_JOIN_TIME] = join_time[algorithm]
        elif algorithm == YannakakisB:
            summary[YannakakisB_JOIN_TIME] = join_time[algorithm]


    bar_plot2(ax, summary, labels,
              colors=['#00994D', '#7fcca6', '#FF9933', '#ffcc99', "#00C3E3", "#7fe1f1"], total_width=.7,
              single_width=1,
              fontdict=font,
              legend=True)

    plt.tight_layout()
    filename = "exp1.3.e.3.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    plot_tpch()
