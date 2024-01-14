"""
Experiment 1.1: Distribution of individual query evaluation time

multiple bar graphs (3) + broken axis (TODO) + label at bottom
"""
from pathlib import Path

import matplotlib
import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import DATA_SOURCE_CSV, ALGORITHMS_TO_PLOT, HJ, TTJ, Yannakakis, COLUMN_RIGHT_BOUND
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / csv_name

def plot_job():
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
        # if legend:
        #     ax.legend(bars, [r'$\mathsf{TTJ}$', r'Yannakakis'], frameon=False, fontsize=20, ncol=3,
        #               loc=(0.9, 0.9))

        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        ax.axhline(y=0, color='#FF0000', linestyle='-')
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
        DATA_SOURCE_CSV: "2023-11-15T07:13:39.823669Z_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis],
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
            [(hj_time - algorithm_time) / hj_time for hj_time, algorithm_time in zip(hj, grouped_data[algorithm])]
    del data_speedup[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    # fig, ax = plt.subplots(figsize=(14, 3), dpi=140)

    fig, axs = plt.subplots(3, 1, figsize=(14, 14), dpi=140)
    # axs[0].plot(t, s1, t, s2)
    # axs[0].set_xlim(0, 2)
    # axs[0].set_xlabel('Time (s)')
    # axs[0].set_ylabel('s1 and s2')
    # axs[0].grid(True)
    #
    # cxy, f = axs[1].cohere(s1, s2, 256, 1. / dt)
    # axs[1].set_ylabel('Coherence')
    #
    # plt.show()

    numdp = len(labels)
    chunk_1_endpoint = round(numdp/3)
    chunk_2_endpoint = chunk_1_endpoint*2
    data_chunk1, data_chunk2, data_chunk3 = dict(), dict(), dict()
    data_chunk1_label, data_chunk2_label, data_chunk3_label = (labels[:chunk_1_endpoint],
                                                               labels[chunk_1_endpoint:chunk_2_endpoint],
                                                               labels[chunk_2_endpoint:])
    for algorithms, dps in data_speedup.items():
        data_chunk1[algorithms] = dps[:chunk_1_endpoint]
        data_chunk2[algorithms] = dps[chunk_1_endpoint:chunk_2_endpoint]
        data_chunk3[algorithms] = dps[chunk_2_endpoint:]

    font = {'family': 'Helvetica',
            'size': 5}
    bar_plot(axs[0], data_chunk1, data_chunk1_label, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    bar_plot(axs[1], data_chunk2, data_chunk2_label, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    bar_plot(axs[2], data_chunk3, data_chunk3_label, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.ylabel('Speedup', fontdict=font2)
    plt.xticks(rotation=270)
    plt.tight_layout()
    filename = "exp1.1-bar-job-3.pdf"
    plt.savefig(filename, format='pdf')
    plt.show()



    # # Fixing random state for reproducibility
    # np.random.seed(19680801)
    #
    # dt = 0.01
    # t = np.arange(0, 30, dt)
    # nse1 = np.random.randn(len(t))  # white noise 1
    # nse2 = np.random.randn(len(t))  # white noise 2
    #
    # # Two signals with a coherent part at 10 Hz and a random part
    # s1 = np.sin(2 * np.pi * 10 * t) + nse1
    # s2 = np.sin(2 * np.pi * 10 * t) + nse2
    #
    # fig, axs = plt.subplots(2, 1, layout='constrained')
    # axs[0].plot(t, s1, t, s2)
    # axs[0].set_xlim(0, 2)
    # axs[0].set_xlabel('Time (s)')
    # axs[0].set_ylabel('s1 and s2')
    # axs[0].grid(True)
    #
    # cxy, f = axs[1].cohere(s1, s2, 256, 1. / dt)
    # axs[1].set_ylabel('Coherence')
    #
    # plt.show()

if __name__ == "__main__":
    plot_job()