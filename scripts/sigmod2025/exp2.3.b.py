"""
Like exp2.3.a but including TTJ_BG as well.
"""

import logging
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import HJ, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, DATA_SOURCE_CSV, TTJ, Yannakakis, TTJ_NO_NG, \
    TTJ_BF, TTJ_BG
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument

# Suppress the findfont warning message
logging.getLogger('matplotlib.font_manager').disabled = True


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / csv_name


def job_speedup():
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
            ax.legend(bars, [r'$\mathsf{TTJ}^{bf}$', r'$\mathsf{TTJ}^{bf+df}$', r'$\mathsf{TTJ}^{bf+ng}$', r'$\mathsf{TTJ}$'], frameon=False,
                      fontsize=15, ncol=2, loc='best')

        if labels is not None:
            x = np.arange(len(labels))
            if fontdict is not None:
                ax.set_xticks(x)
                ax.set_xticklabels(labels, fontproperties=fontdict)
            else:
                ax.set_xticks(x, labels)
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 20}
        ax.text(0.05, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        if labels is None:
            ax.set_xticklabels([])
            ax.set_xticks([])
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
        DATA_SOURCE_CSV: "2023-11-30T01:12:54.057467Z_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, TTJ_NO_NG, TTJ_BF, TTJ_BG],
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

    data_speedup = dict()
    for algorithm, dps in grouped_data.items():
        hj = grouped_data[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, grouped_data[algorithm])]

    data_speedup_usefulness = dict()
    data_speedup_usefulness[TTJ_BF] = []
    data_speedup_usefulness[TTJ_NO_NG] = []
    data_speedup_usefulness[TTJ] = []
    data_speedup_usefulness[TTJ_BG] = []
    def analyze_data_speedup():
        bf_useful = []
        bd_useful = []
        bg_useful = []
        nf_useful = []
        none_useful = []
        selected_data_idx = []
        for i, (hj_dp, bf_dp, bg_dp, bd_dp, ttj_dp) in enumerate(
                zip(data_speedup[HJ], data_speedup[TTJ_BF], data_speedup[TTJ_BG], data_speedup[TTJ_NO_NG], data_speedup[TTJ])):
            if bf_dp > hj_dp:
                bf_useful.append(labels[i])
                if i not in selected_data_idx:
                    selected_data_idx.append(i)
            if bd_dp > bf_dp:
                bd_useful.append(labels[i])
                if i not in selected_data_idx:
                    selected_data_idx.append(i)
            if bg_dp > bf_dp:
                bg_useful.append(labels[i])
                if i not in selected_data_idx:
                    selected_data_idx.append(i)
            if ttj_dp > bd_dp:
                nf_useful.append(labels[i])
                if i not in selected_data_idx:
                    selected_data_idx.append(i)
            if bf_dp < hj_dp and bd_dp < hj_dp and ttj_dp < hj_dp:
                none_useful.append(labels[i])
            if bd_dp < bf_dp:
                print(f"label: {labels[i]}")

        for i, (hj_dp, bf_dp, bg_dp, bd_dp, ttj_dp) in enumerate(
                zip(data_speedup[HJ], data_speedup[TTJ_BF], data_speedup[TTJ_BG], data_speedup[TTJ_NO_NG], data_speedup[TTJ])):
            if i in selected_data_idx:
                data_speedup_usefulness[TTJ_BF].append(bf_dp)
                data_speedup_usefulness[TTJ_NO_NG].append(bd_dp)
                data_speedup_usefulness[TTJ_BG].append(bg_dp)
                data_speedup_usefulness[TTJ].append(ttj_dp)
        print(f"bf_useful: {bf_useful}")
        print(f"bd_useful: {bd_useful}")
        print(f"bg_useful: {bg_useful}")
        print(f"nf_useful: {nf_useful}")
        print(f"none_useful: {none_useful}")
        labels_to_print = []
        candidate_labels = []
        for label in bf_useful + bd_useful + nf_useful + bg_useful:
            if label not in candidate_labels:
                candidate_labels.append(label)
        for label in labels:
            if label in candidate_labels:
                labels_to_print.append(label)
        return labels_to_print
    labels_to_print = analyze_data_speedup()
    print(f"positively contribute to {len(labels_to_print)} queries in JOB")

    data_speedup_use = dict()
    del data_speedup[HJ]
    for algorithm in [TTJ_BF, TTJ_NO_NG, TTJ_BG, TTJ]:
        data_speedup_use[algorithm] = data_speedup_usefulness[algorithm]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 3), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 7}
    bar_plot(ax, data_speedup_use, labels=labels_to_print, colors=['#FF9933', '#9933FF', '#0080FF', '#00994D'], total_width=.7,
             single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 10}
    plt.ylabel('Speedup', fontdict=font2)
    plt.margins(x=0.01)
    plt.xticks(rotation=270)
    plt.tight_layout()
    filename = "exp2.3.b-job.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()


if __name__ == "__main__":
    job_speedup()
