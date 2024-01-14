"""
Plot TPC-H related charts.
"""
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt, transforms, ticker

from plot.constants import DATA_SOURCE_CSV, DECIMAL_PRECISION, HJ, PLOT_FUNCTION, FIG_SAVE_LOCATION, \
    SORT_DESCENDING_BASED_ON_HJ, COLUMN_RIGHT_BOUND
from plot.job import extract_data_from_csv, construct_fig_name
from plot.utility import check_argument, convert_time, TimeUnits


def tpch_speedup(plot_conf: dict):
    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        # patterns = ["|", "\\", "/", "+", "-", ".", "*", "x", "o", "O"]
        patterns = ["\\", "-", "/", ""]

        # Check if colors where provided, otherwhise use the default color cycle
        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

        # Number of bars per group
        n_bars = len(data)

        # The width of a single bar
        bar_width = total_width / n_bars

        # List containing handles for the drawn bars, used for the legend
        bars = []

        # Iterate over all data
        for i, (name, values) in enumerate(data.items()):
            # The offset in x direction of that bar
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2

            # Draw a bar for every value of that type
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
                # The following is about speedup
                # if name == HJ:
                #     plt.text(x=x + x_offset, y=y+1, s=f"{speedup[x]}", fontdict=fontdict,
                #              rotation=90)

            # Add a handle to the last drawn bar, which we'll need for the legend
            bars.append(bar[0])

        # Draw legend if we need
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$', r'$\mathsf{LIP}$',
                             r'Yannakakis'], fontsize=20, ncol=3)

        ax.set_yscale('log')
        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        # draw an invisible line to make legend not block bars
        plt.axhline(y=1.5, color='#FFFFFF', linestyle='-')
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

    sort_descending_based_on_hj = True
    data, original_idx = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]),
                                               column_range=[1, 16],
                                               sort_descending_based_on_hj=sort_descending_based_on_hj)
    check_argument(len(data.keys()) == 4, f"there should be 4 algorithms in data. Current only have {data.keys()}")
    for dps in data.values():
        check_argument(len(dps) == 15,
                       f"some query data is missing. There should be 18 dps. Instead, we have {len(dps)}")

    labels = ["Q2", "Q3", "Q9", "Q10", "Q11", "Q12", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22"]
    if sort_descending_based_on_hj:
        labels = [labels[i] for i in original_idx]

    zero_dp_idx = []
    for algorithm, dps in data.items():
        for i in range(len(dps)):
            if i not in zero_dp_idx and dps[i] == 0:
                zero_dp_idx.append(i)
    labels = [v for i, v in enumerate(labels) if i not in zero_dp_idx]
    for algorithm in data.keys():
        data[algorithm] = [v for i, v in enumerate(data[algorithm]) if i not in zero_dp_idx]

    data_converted = dict()
    global_max = 0
    global_min = 99999999
    for algorithm, dps in data.items():
        data_converted[algorithm] = [round(dp, DECIMAL_PRECISION) for dp in convert_time(dps, TimeUnits.SECONDS)]
        global_max = max(global_max, max(data_converted[algorithm]))
        global_min = min(global_min, min(data_converted[algorithm]))

    data_speedup = dict()
    for algorithm, dps in data_converted.items():
        hj = data_converted[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, data_converted[algorithm])]
    del data_speedup[HJ]

    plt.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 6), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    bar_plot(ax, data_speedup, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.ylabel('Speedup in log scale', fontdict=font2)
    plt.tight_layout()
    if sort_descending_based_on_hj:
        filename = "tpch_speedup_sorted.pdf"
    else:
        filename = "tpch_speedup.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def tpch_speedup_with_predicates(plot_conf: dict):
    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        # patterns = ["|", "\\", "/", "+", "-", ".", "*", "x", "o", "O"]
        patterns = ["\\", "-", "/", ""]

        # Check if colors where provided, otherwhise use the default color cycle
        if colors is None:
            colors = plt.rcParams['axes.prop_cycle'].by_key()['color']

        # Number of bars per group
        n_bars = len(data)

        # The width of a single bar
        bar_width = total_width / n_bars

        # List containing handles for the drawn bars, used for the legend
        bars = []

        # Iterate over all data
        for i, (name, values) in enumerate(data.items()):
            # The offset in x direction of that bar
            x_offset = (i - n_bars / 2) * bar_width + bar_width / 2

            # Draw a bar for every value of that type
            for x, y in enumerate(values):
                bar = ax.bar(x + x_offset, y, width=bar_width * single_width,
                             color=colors[i % len(colors)], hatch=patterns[i],
                             edgecolor='black')
                # The following is about speedup
                # if name == HJ:
                #     plt.text(x=x + x_offset, y=y+1, s=f"{speedup[x]}", fontdict=fontdict,
                #              rotation=90)

            # Add a handle to the last drawn bar, which we'll need for the legend
            bars.append(bar[0])

        # Draw legend if we need
        if legend:
            ax.legend(bars, [r'$\mathsf{TTJ}$'], fontsize=20, ncol=3, frameon=False,
                      loc=(0.8, 0.90))

        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        plt.axhline(y=1, color='#FF0000', linestyle='-')
        # draw an invisible line to make legend not block bars
        plt.axhline(y=1.5, color='#FFFFFF', linestyle='-')
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 20}
        ax.text(0.08, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        ax.minorticks_off()
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)
        ax.set_ylim(0, 1.2)

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    data, original_idx = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]),
                                               column_range=[1, plot_conf[COLUMN_RIGHT_BOUND]],
                                               sort_descending_based_on_hj=plot_conf[SORT_DESCENDING_BASED_ON_HJ])
    labels = ["3", "7", "8", "9", "10", "11", "12", "14", "15", "16", "18", "19", "20"]
    if plot_conf[SORT_DESCENDING_BASED_ON_HJ]:
        labels = [labels[i] for i in original_idx]

    check_argument(len(data.keys()) == 2, f"there should be 2  algorithms in data. Current only have {data.keys()}")
    for dps in data.values():
        check_argument(len(dps) == len(labels),
                       f"some query data is missing. There should be {len(labels)} dps. Instead, we have {len(dps)}")

    zero_dp_idx = []
    for algorithm, dps in data.items():
        for i in range(len(dps)):
            if i not in zero_dp_idx and dps[i] == 0:
                zero_dp_idx.append(i)
    labels = [v for i, v in enumerate(labels) if i not in zero_dp_idx]
    for algorithm in data.keys():
        data[algorithm] = [v for i, v in enumerate(data[algorithm]) if i not in zero_dp_idx]

    data_speedup = dict()
    for algorithm, dps in data.items():
        hj = data[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, data[algorithm])]
    del data_speedup[HJ]

    plt.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 4), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    bar_plot(ax, data_speedup, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
             'weight': 'bold',
             'size': 20}
    plt.ylabel('Speedup', fontdict=font2)
    plt.tight_layout()
    if plot_conf[SORT_DESCENDING_BASED_ON_HJ]:
        filename = "tpch_speedup_with_predicates_sorted.pdf"
    else:
        filename = "tpch_speedup_with_predicates.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def driver(plot_conf: dict):
    """
    Invoke the necessary plotting function
    """
    for func in plot_conf[PLOT_FUNCTION]:
        func(plot_conf)


if __name__ == "__main__":
    tpch_plot = {
        DATA_SOURCE_CSV: "TPCH-Performance-Summary-overview.csv",
        PLOT_FUNCTION: [tpch_speedup],
        FIG_SAVE_LOCATION: r"../../tex/img"
    }

    tpch_plot_with_predicates = {
        DATA_SOURCE_CSV: "benchmarktpchwithpredicatesdifferentordering-result-2023-07-06t17:42:23.652904benchmarktpchwithpredicatesdifferentordering-result-2023-07-07t13:48:17.442581_perf_report.csv",
        PLOT_FUNCTION: [tpch_speedup_with_predicates],
        FIG_SAVE_LOCATION: r".",
        SORT_DESCENDING_BASED_ON_HJ: False,
        COLUMN_RIGHT_BOUND: 14,
    }

    driver(plot_conf=tpch_plot_with_predicates)