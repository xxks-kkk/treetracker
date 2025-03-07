"""
Plot job-related charts
"""
import csv
from pathlib import Path
from typing import Tuple, List

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib import ticker, transforms

from plot.constants import DATA_SOURCE_CSV, HJ, TTJ, LIP, Yannakakis, FIG_SAVE_LOCATION, DECIMAL_PRECISION, \
    PLOT_FUNCTION, ALGORITHMS_TO_PLOT, YannakakisB, YannakakisV, PTO, Yannakakis1Pass, TTJ_VANILLA, TTJ_NO_DP, TTJ_NO_NG
from plot.utility import check_argument, TimeUnits, convert_time, normalized_data, to_float


def extract_data_from_csv(csv_file_path: Path,
                          column_range: List[int],
                          sort_descending_based_on_hj: bool = False) -> Tuple[dict,List]:
    """
    Extract the data from csv. The result is
    {hj: [...], ttj: [...], lip: [...], yannakakis: [...]}

    sort_descending_based_on_hj when set to True means all the data are sorted based on the runtime of HJ in
    descending order.
    """
    start_idx = column_range[0]
    end_idx = column_range[1]
    result = dict()
    with open(csv_file_path, "r") as file:
        csv_file = csv.reader(file)
        for row in csv_file:
            if HJ in row:
                result[HJ] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif TTJ in row:
                result[TTJ] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif TTJ_VANILLA in row:
                result[TTJ_VANILLA] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif TTJ_NO_DP in row:
                result[TTJ_NO_DP] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif TTJ_NO_NG in row:
                result[TTJ_NO_NG] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif LIP in row:
                result[LIP] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif Yannakakis in row and YannakakisB not in row and YannakakisV not in row:
                result[Yannakakis] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif YannakakisB in row:
                result[YannakakisB] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif YannakakisV in row:
                result[YannakakisV] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif Yannakakis1Pass in row:
                result[Yannakakis1Pass] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
            elif PTO in row:
                result[PTO] = [to_float(num_str) for num_str in row[start_idx:end_idx]]
    original_idx = None
    if sort_descending_based_on_hj:
        for algorithm in result.keys():
            if algorithm == HJ:
                original_idx = np.array(result[algorithm]).argsort()[::-1]
                result[algorithm] = sorted(result[algorithm], reverse=True)
            else:
                result[algorithm] = [result[algorithm][i] for i in original_idx]
    return result, original_idx


def construct_fig_name(plot_conf: dict, img_name: str) -> str:
    absolute_path = Path(plot_conf[FIG_SAVE_LOCATION]).resolve()
    if not absolute_path.exists():
        absolute_path.mkdir(parents=True, exist_ok=True)
    return absolute_path.joinpath(img_name).__str__()


def job_performance(plot_conf: dict):
    DECIMAL_PRECISION = 2

    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        """Draws a bar plot with multiple bars per data point.

        Parameters
        ----------
        ax : matplotlib.pyplot.axis
            The axis we want to draw our plot on.

        data: dictionary
            A dictionary containing the data we want to plot. Keys are the names of the
            data, the items is a list of the values.

            Example:
            data = {
                "x":[1,2,3],
                "y":[1,2,3],
                "z":[1,2,3],
            }

        colors : array-like, optional
            A list of colors which are used for the bars. If None, the colors
            will be the standard matplotlib color cyle. (default: None)

        total_width : float, optional, default: 0.8
            The width of a bar group. 0.8 means that 80% of the x-axis is covered
            by bars and 20% will be spaces between the bars.

        single_width: float, optional, default: 1
            The relative width of a single bar within a group. 1 means the bars
            will touch eachother within a group, values less than 1 will make
            these bars thinner.

        legend: bool, optional, default: True
            If this is set to true, a legend will be added to the axis.

        fontdict:
            font properties

        Credits: https://stackoverflow.com/a/60270421/1460102
        """

        hj = data[HJ]
        ttj = data[TTJ]
        speedup = [round(hj_time / ttj_time, 1) for hj_time, ttj_time in zip(hj, ttj)]

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
            ax.legend(bars, [r'$\mathsf{HJ}$', r'$\mathsf{TTJ}$', r'$\mathsf{LIP}$',
                             r'Yannakakis'])

        ax.set_yscale('log')
        x = np.arange(len(labels))
        if fontdict is not None:
            ax.set_xticks(x)
            ax.set_xticklabels(labels, fontproperties=fontdict)
        else:
            ax.set_xticks(x, labels)
        # ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.minorticks_off()
        # ax.set_yticks(np.arange(0, 50, step=20))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        # y_major = ticker.LogLocator(base=10.0, numticks=5)
        # y_major = ticker.FixedLocator(ax.get_ylim())
        # ax.yaxis.set_major_locator(y_major)
        # ax.xaxis.set_major_locator(MultipleLocator(20))
        # ax.xaxis.set_major_formatter(FormatStrFormatter('%d'))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)

        show_speedup = False
        if show_speedup:
            for i, pos in enumerate(x):
                plt.text(x=pos, y=230, s=f"{speedup[i]}", fontdict=fontdict)

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    data, _ = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]), column_range=[1,34])
    check_argument(len(data.keys()) == 4, f"there should be 4 algorithms in data. Current only have {data.keys()}")
    for dps in data.values():
        check_argument(len(dps) == 33,
                       f"some query data is missing. There should be 33 dps. Instead, we have {len(dps)}")

    labels = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30",
              "Q31", "Q32", "Q33"]

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
        data_converted[algorithm] = [round(dp, DECIMAL_PRECISION) for dp in convert_time(dps, TimeUnits.MINUTES)]
        global_max = max(global_max, max(data_converted[algorithm]))
        global_min = min(global_min, min(data_converted[algorithm]))

    data_normalized = dict()
    for algorithm, dps in data_converted.items():
        data_normalized[algorithm] = normalized_data(dps, global_min, global_max)

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 6), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 12}
    bar_plot(ax, data_converted, colors=['#FF0000', '#00994D', '#007FFF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    plt.ylabel('execution time (min) in log scale', fontdict=font)
    plt.tight_layout()
    filename = construct_fig_name(plot_conf, "job_performance.pdf")
    plt.savefig(filename, format='pdf')
    plt.show()


def job_speedup(plot_conf: dict):
    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        """Draws a bar plot with multiple bars per data point.

        Parameters
        ----------
        ax : matplotlib.pyplot.axis
            The axis we want to draw our plot on.

        data: dictionary
            A dictionary containing the data we want to plot. Keys are the names of the
            data, the items is a list of the values.

            Example:
            data = {
                "x":[1,2,3],
                "y":[1,2,3],
                "z":[1,2,3],
            }

        colors : array-like, optional
            A list of colors which are used for the bars. If None, the colors
            will be the standard matplotlib color cyle. (default: None)

        total_width : float, optional, default: 0.8
            The width of a bar group. 0.8 means that 80% of the x-axis is covered
            by bars and 20% will be spaces between the bars.

        single_width: float, optional, default: 1
            The relative width of a single bar within a group. 1 means the bars
            will touch eachother within a group, values less than 1 will make
            these bars thinner.

        legend: bool, optional, default: True
            If this is set to true, a legend will be added to the axis.

        fontdict:
            font properties

        Credits: https://stackoverflow.com/a/60270421/1460102
        """
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
                             r'Yannakakis'], frameon=False, fontsize=20, ncol=3)

        ax.set_yscale('log')
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
        # ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.minorticks_off()
        # ax.set_yticks(np.arange(0, 50, step=20))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        # y_major = ticker.LogLocator(base=10.0, numticks=5)
        # y_major = ticker.FixedLocator(ax.get_ylim())
        # ax.yaxis.set_major_locator(y_major)
        # ax.xaxis.set_major_locator(MultipleLocator(20))
        # ax.xaxis.set_major_formatter(FormatStrFormatter('%d'))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    sort_descending_based_on_hj = True
    data, original_idx = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]),
                                               column_range=[1,34],
                                               sort_descending_based_on_hj=sort_descending_based_on_hj)
    check_argument(len(data.keys()) == 4, f"there should be 4 algorithms in data. Current only have {data.keys()}")
    for dps in data.values():
        check_argument(len(dps) == 33,
                       f"some query data is missing. There should be 33 dps. Instead, we have {len(dps)}")

    labels = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30",
              "Q31", "Q32", "Q33"]
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
        data_converted[algorithm] = [round(dp, DECIMAL_PRECISION) for dp in convert_time(dps, TimeUnits.MINUTES)]
        global_max = max(global_max, max(data_converted[algorithm]))
        global_min = min(global_min, min(data_converted[algorithm]))

    data_speedup = dict()
    for algorithm, dps in data_converted.items():
        hj = data_converted[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, data_converted[algorithm])]
    del data_speedup[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
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
        filename = "job_speedup_sorted.pdf"
    else:
        filename = "job_speedup.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def plot_execution_time_breakddown_by_query_expense(plot_conf: dict):
    """
    Like Ding2020 Figure 8, we classify queries into cheap queries, normal queries, and expensive queries using hash join.
    And we break down execution time by these three categories.
    """
    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None, add_bar_label=True):
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
                if add_bar_label:
                    ax.bar_label(bar, label_type='edge', padding=3, fontsize=20)
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
        trans = transforms.blended_transform_factory(
            ax.get_yticklabels()[0].get_transform(), ax.transData)
        font = {'family': 'Helvetica',
                'weight': 'bold',
                'size': 20}
        ax.text(0.05, 1.1, r"$\mathsf{HJ}$", color="#FF0000", transform=trans,
                ha="right", va="center", fontdict=font)
        # ax.yaxis.set_major_formatter(ScalarFormatter())
        ax.minorticks_off()
        # ax.set_yticks(np.arange(0, 50, step=20))
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:g}'.format(y)))
        # y_major = ticker.LogLocator(base=10.0, numticks=5)
        # y_major = ticker.FixedLocator(ax.get_ylim())
        # ax.yaxis.set_major_locator(y_major)
        # ax.xaxis.set_major_locator(MultipleLocator(20))
        # ax.xaxis.set_major_formatter(FormatStrFormatter('%d'))
        plt.grid(which='major', axis='y', zorder=-1.0)
        ax.set_axisbelow(True)

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    sort_descending_based_on_hj = True
    data, original_idx = extract_data_from_csv(Path(plot_conf[DATA_SOURCE_CSV]), sort_descending_based_on_hj)
    check_argument(len(data.keys()) == 4, f"there should be 4 algorithms in data. Current only have {data.keys()}")
    expected_dps_len = 33
    for dps in data.values():
        check_argument(len(dps) == expected_dps_len,
                       f"some query data is missing. There should be 33 dps. Instead, we have {len(dps)}")

    plot_data = dict()
    labels = ['Expensive queries', 'Normal queries', 'Cheap queries']
    expensive_queres = dict()
    for algorithm, dps in data.items():
        expensive_queres[algorithm] = dps[:int(expected_dps_len/3)]
        if algorithm not in plot_data:
            plot_data[algorithm] = []
        plot_data[algorithm].append(sum(expensive_queres[algorithm]))

    middle_queries = dict()
    for algorithm, dps in data.items():
        middle_queries[algorithm] = dps[int(expected_dps_len/3): int(expected_dps_len/3*2)]
        if algorithm not in plot_data:
            plot_data[algorithm] = []
        plot_data[algorithm].append(sum(middle_queries[algorithm]))

    cheap_queries = dict()
    for algorithm, dps in data.items():
        cheap_queries[algorithm] = dps[int(expected_dps_len/3*2):]
        if algorithm not in plot_data:
            plot_data[algorithm] = []
        plot_data[algorithm].append(sum(cheap_queries[algorithm]))

    data_converted = dict()
    global_max = 0
    global_min = 99999999
    for algorithm, dps in plot_data.items():
        data_converted[algorithm] = [round(dp, DECIMAL_PRECISION) for dp in convert_time(dps, TimeUnits.MINUTES)]
        global_max = max(global_max, max(data_converted[algorithm]))
        global_min = min(global_min, min(data_converted[algorithm]))

    data_normalized = dict()
    for algorithm, dps in data_converted.items():
        hj = data_converted[HJ]
        data_normalized[algorithm] = [round(algorithm_time / hj_time, 1) for hj_time, algorithm_time in zip(hj, data_converted[algorithm])]
    del data_normalized[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 6), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    bar_plot(ax, data_normalized, colors=['#00994D', '#9933FF', '#FF9933'], total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    plt.ylabel('Normalized execution time in log scale', fontdict=font2)
    plt.tight_layout()
    filename = "execution_time_breakddown_by_query_expense2.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def plot_impact_of_backjump_scale_with_backjumped_relation_size(plot_conf: dict):
    including_yannakakis = False

    scale_factors = [256, 1024, 4096, 16384, 65536]
    ttjhp = [13.256, 13.907, 15.669, 25.343, 51.369]
    hj = [29.073, 130.038, 1823.784, 23213.257, 357585.575]
    lip = [27.359, 152.077, 1801.32, 22163.812, 363564.497]
    if including_yannakakis:
        yannakakis = [0.100, 0.099, 0.116, 0.098, 0.104]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=10)
    ax.minorticks_off()
    ax.plot(scale_factors, ttjhp, marker='o', color='#00994D', label=r'$\mathsf{TTJ}$')
    ax.plot(scale_factors, hj, marker='*', color='#FF0000', label=r"$\mathsf{HJ}$")
    ax.plot(scale_factors, lip, marker='x', color='#9933FF', label=r"$\mathsf{LIP}$")
    if including_yannakakis:
        ax.plot(scale_factors, yannakakis, marker='+', color='#FF9933', label=r'Yannakakis')
    plt.grid(which='major', axis='both', zorder=-1.0)
    font2 = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    plt.ylabel('Execution time (ms) in log scale', fontdict=font2)
    plt.xlabel(r'$n$', fontdict=font2)
    ax.set_axisbelow(True)
    plt.tight_layout()
    if including_yannakakis:
        plt.legend(fontsize=15)
    else:
        plt.legend(fontsize=20)
    filename = "impact_of_backjump_scale_with_backjumped_relation_size.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()


def plot_impact_of_backjump_scale_with_number_of_backjump_relations(plot_conf: dict):
    including_yannakakis = False
    scale_factors = [1, 2, 4, 8, 16, 32]
    ttjhp = [19.326, 25.771, 37.296, 49.274, 84.445, 127.425]
    hj = [22.586, 30.277, 41.087, 51.261, 143.263, 812749.184]
    lip = [22.498, 27.763, 41.045, 58.264, 152.065, 849591.588]
    if including_yannakakis:
        yannakakis = [25.812, 26.906, 38.085, 74.765, 91.818, 133.235]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(6, 6), dpi=140)
    ax.set_xscale('log', base=2)
    ax.set_yscale('log', base=10)
    ax.minorticks_off()
    ax.plot(scale_factors, ttjhp, marker='o', color='#00994D', label=r'$\mathsf{TTJ}$')
    ax.plot(scale_factors, hj, marker='*', color='#FF0000', label=r"$\mathsf{HJ}$")
    ax.plot(scale_factors, lip, marker='x', color='#9933FF', label=r"$\mathsf{LIP}$")
    if including_yannakakis:
        ax.plot(scale_factors, yannakakis, marker='+', color='#FF9933', label=r'Yannakakis')
    plt.grid(which='major', axis='both', zorder=-1.0)
    font2 = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    plt.ylabel('Execution time (ms) in log scale', fontdict=font2)
    plt.xlabel(r'$k$', fontdict=font2)
    ax.set_axisbelow(True)
    plt.tight_layout()
    if including_yannakakis:
        plt.legend(fontsize=15)
    else:
        plt.legend(fontsize=20)
    filename = "impact_of_backjump_scale_with_number_of_backjump_relations.pdf"
    file_path = construct_fig_name(plot_conf, filename)
    plt.savefig(file_path, format='pdf')
    plt.show()

def plot_algorithm_overhead_illustration(plot_conf : dict):
    def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True, fontdict=None):
        patterns = []
        legend_text = []
        for algorithm in plot_conf[ALGORITHMS_TO_PLOT]:
            if algorithm == TTJ:
                patterns.append("\\")
                legend_text.append(r'$\mathsf{TTJ}$')
            elif algorithm == LIP:
                patterns.append("-")
                legend_text.append(r'$\mathsf{LIP}$')
            elif algorithm == Yannakakis:
                patterns.append("/")
                legend_text.append(r'Yannakakis')

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
                ax.bar_label(bar, label_type='edge', padding=9, fontsize=20)
            bars.append(bar[0])

        if legend:
            ax.legend(bars, legend_text, fontsize=20, ncol=len(legend_text), frameon=False, loc=(0.02, 0.85))

        ax.set_yscale('log')
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

        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)

    data_all = {
        HJ: [12350, 10470000],
        TTJ: [12610, 108000],
        LIP: [14470, 13356000],
        Yannakakis: [70670, 1116600]
    }

    colors = []
    data = dict()
    for algorithm in plot_conf[ALGORITHMS_TO_PLOT]:
        data[algorithm] = data_all[algorithm]
        if algorithm == TTJ:
            colors.append('#00994D')
        elif algorithm == LIP:
            colors.append('#9933FF')
        elif algorithm == Yannakakis:
            colors.append('#FF9933')

    labels = ["SSB Q4", "JOB Q30"]

    zero_dp_idx = []
    for algorithm, dps in data.items():
        for i in range(len(dps)):
            if i not in zero_dp_idx and dps[i] == 0:
                zero_dp_idx.append(i)
    labels = [v for i, v in enumerate(labels) if i not in zero_dp_idx]
    for algorithm in data.keys():
        data[algorithm] = [v for i, v in enumerate(data[algorithm]) if i not in zero_dp_idx]

    data_converted = dict()
    for algorithm, dps in data.items():
        data_converted[algorithm] = [round(dp, DECIMAL_PRECISION) for dp in convert_time(dps, TimeUnits.MINUTES)]

    data_speedup = dict()
    for algorithm, dps in data_converted.items():
        hj = data_converted[HJ]
        data_speedup[algorithm] = \
            [round(hj_time / algorithm_time, 1) for hj_time, algorithm_time in zip(hj, data_converted[algorithm])]
    del data_speedup[HJ]

    matplotlib.rcParams['mathtext.fontset'] = 'stix'
    fig, ax = plt.subplots(figsize=(14, 6), dpi=140)
    font = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 15}
    bar_plot(ax, data_speedup, colors=colors, total_width=.7, single_width=1,
             fontdict=font)
    font2 = {'family': 'Helvetica',
            'weight': 'bold',
            'size': 20}
    plt.ylabel('Speedup in log scale', fontdict=font2)
    plt.tight_layout()
    filename = "algorithm_overhead_illustration_speedup.pdf"
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
    job_plot = {
        DATA_SOURCE_CSV: "JOB-Performance-Production-Run-overview.csv",
        PLOT_FUNCTION: [job_speedup],
        FIG_SAVE_LOCATION: r"../../tex/img",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis]
    }
    driver(plot_conf=job_plot)
