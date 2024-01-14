"""
Plot JOB execution time results
"""
from typing import List

import matplotlib.pyplot as plt
import numpy as np

from plot.utility import check_argument
from validation_error import ValidationError


class TimeUnits:
    SECONDS = "SECONDS"
    MINUTES = "MINUTES"


def convert_time(data: List[float], unit: str) -> List[float]:
    if unit == TimeUnits.SECONDS:
        return [dp / 1000 for dp in data]
    if unit == TimeUnits.MINUTES:
        return [dp / 1000 / 60 for dp in data]
    raise ValidationError(f"Given unit: {unit} is unsupported")


def find_non_zero_min(data: List[float]) -> float:
    non_zero_min = 999999999
    for dp in data:
        if dp != 0 and dp < non_zero_min:
            non_zero_min = dp
    return non_zero_min


def normalized_data(data: List[float], min_all: float, max_all: float) -> List[float]:
    return [(dp - min_all) / (max_all - min_all) for dp in data]


def logscale_data(data: List[float]) -> List[float]:
    return [np.log(dp) for dp in data]

def bar_plot(ax, data, colors=None, total_width=0.8, single_width=1, legend=True):
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
    """

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
            bar = ax.bar(x + x_offset, y, width=bar_width * single_width, color=colors[i % len(colors)])

        # Add a handle to the last drawn bar, which we'll need for the legend
        bars.append(bar[0])

    # Draw legend if we need
    if legend:
        ax.legend(bars, data.keys())

    ax.set_yscale('log')
    labels = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30", "Q31",
              "Q32", "Q33"]
    x = np.arange(len(labels))
    ax.set_xticks(x, labels)

def plot():
    labels = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30", "Q31",
              "Q32", "Q33"]
    hj_time_raw = [5692.592, 19770.182, 68953.589, 7038.185, 35680.056, 139277.89, 338242.327, 150411.577, 105522.527,
                   96694.08, 14866.837, 32247.927, 31997.949, 19574.518, 2721267.835, 1811991.514, 1005483.542, 0,
                   1277228.576, 107865.167, 979889.034, 744951.277, 1384755.648, 0, 8004635.372, 129846.219, 113972.074,
                   970952.4, 0, 10466725.68, 0, 2802.248, 4309.092464]
    ttj_time_raw = [5854.4, 21987.404, 73884.01, 7361.372, 39496.423, 149430.538, 61445.287, 127231.712, 98503.482,
                    71501.422, 6755.391, 16800.322, 16845.532, 13143.701, 3448458.578, 1170008.858, 1036009.349, 0,
                    1611318.597, 89627.186, 421262.049, 12681.318, 1883510.001, 0, 52529.775, 144920.125, 136802.21,
                    11804.866, 0, 106051.853, 0, 2802.425, 4634.756037]
    lip_time_raw = [7162.702, 22750.656, 80040.082, 7824.328, 42924.785, 155352.262, 58660.311, 166033.293, 132564.09,
                    85536.746, 7852.384, 34858.254, 35774.06, 23777.129, 2553029.646, 1857042.289, 1015339.206, 0,
                    1506129.121, 126722.885, 363483.812, 717237.824, 1458560.147, 0, 8355419.293, 150769.562,
                    122618.925,
                    1038170.056, 0, 13357413.21, 0, 4499.773, 5847.207379]
    yannakakis_time_raw = [15986.847, 41318.331, 65192.436, 20329.379, 51074.502, 239015.242, 71963.84, 204296.136,
                           173354.361, 220609.033, 22177.698, 29159.732, 28548.85, 17585.668, 266415.782, 884742.599,
                           1179031.569, 216480.1, 385777.039, 183015.023, 57612.912, 178438.988, 258448.647, 0,
                           196880.267,
                           219173.528, 39701.25, 190893.9, 17936310.24, 1116831.307, 0, 11891.249, 13114.30292]
    check_argument(
        len(ttj_time_raw) == len(hj_time_raw) and
        len(hj_time_raw) == len(lip_time_raw) and
        len(lip_time_raw) == len(yannakakis_time_raw),
        f"len(ttj_time): {len(ttj_time_raw)}\n"
        f"len(hj_time): {len(hj_time_raw)}\n"
        f"len(lip_time): {len(lip_time_raw)}\n"
        f"len(yannakakis_time): {len(yannakakis_time_raw)}")

    hj_time = convert_time(hj_time_raw, TimeUnits.MINUTES)
    hj_time = [round(dp, 2) for dp in hj_time]
    print(f"hj_time (min): {hj_time}")
    ttj_time = convert_time(ttj_time_raw, TimeUnits.MINUTES)
    ttj_time = [round(dp, 2) for dp in ttj_time]
    print(f"ttj_time (min): {ttj_time}")
    lip_time = convert_time(lip_time_raw, TimeUnits.MINUTES)
    lip_time = [round(dp, 2) for dp in lip_time]
    print(f"lip_time (min): {lip_time}")
    yannakakis_time = convert_time(yannakakis_time_raw, TimeUnits.MINUTES)
    yannakakis_time = [round(dp, 2) for dp in yannakakis_time]
    print(f"yannakakis_time (min): {yannakakis_time}")

    all_data = []
    all_data.extend(hj_time)
    all_data.extend(ttj_time)
    all_data.extend(lip_time)
    all_data.extend(yannakakis_time)
    min_all_data = min(all_data)
    max_all_data = max(all_data)

    hj_time_normalized = normalized_data(hj_time, min_all_data, max_all_data)
    ttj_time_normalized = normalized_data(ttj_time, min_all_data, max_all_data)
    lip_time_normalized = normalized_data(lip_time, min_all_data, max_all_data)
    yannakakis_time_normalized = normalized_data(yannakakis_time, min_all_data, max_all_data)
    data = {
        # "a": [1, 2, 3, 2, 1],
        # "b": [2, 3, 4, 3, 1],
        # "c": [3, 2, 1, 4, 2],
        # "d": [5, 9, 2, 1, 8],
        # "e": [1, 3, 2, 2, 3],
        # "f": [4, 3, 1, 1, 4],
        "hj": hj_time_normalized,
        "ttj": ttj_time_normalized,
        "lip": lip_time_normalized,
        "yannakakis": yannakakis_time_normalized
    }

    fig, ax = plt.subplots(figsize=(12, 6), dpi=140)
    print(hj_time_normalized)
    bar_plot(ax, data, total_width=.85, single_width=1)
    plt.show()

def plot3():
    labels = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30", "Q31",
              "Q32", "Q33"]
    hj_time_raw = [5692.592, 19770.182, 68953.589, 7038.185, 35680.056, 139277.89, 338242.327, 150411.577, 105522.527,
                   96694.08, 14866.837, 32247.927, 31997.949, 19574.518, 2721267.835, 1811991.514, 1005483.542, 0,
                   1277228.576, 107865.167, 979889.034, 744951.277, 1384755.648, 0, 8004635.372, 129846.219, 113972.074,
                   970952.4, 0, 10466725.68, 0, 2802.248, 4309.092464]
    ttj_time_raw = [5854.4, 21987.404, 73884.01, 7361.372, 39496.423, 149430.538, 61445.287, 127231.712, 98503.482,
                    71501.422, 6755.391, 16800.322, 16845.532, 13143.701, 3448458.578, 1170008.858, 1036009.349, 0,
                    1611318.597, 89627.186, 421262.049, 12681.318, 1883510.001, 0, 52529.775, 144920.125, 136802.21,
                    11804.866, 0, 106051.853, 0, 2802.425, 4634.756037]
    lip_time_raw = [7162.702, 22750.656, 80040.082, 7824.328, 42924.785, 155352.262, 58660.311, 166033.293, 132564.09,
                    85536.746, 7852.384, 34858.254, 35774.06, 23777.129, 2553029.646, 1857042.289, 1015339.206, 0,
                    1506129.121, 126722.885, 363483.812, 717237.824, 1458560.147, 0, 8355419.293, 150769.562,
                    122618.925,
                    1038170.056, 0, 13357413.21, 0, 4499.773, 5847.207379]
    yannakakis_time_raw = [15986.847, 41318.331, 65192.436, 20329.379, 51074.502, 239015.242, 71963.84, 204296.136,
                           173354.361, 220609.033, 22177.698, 29159.732, 28548.85, 17585.668, 266415.782, 884742.599,
                           1179031.569, 216480.1, 385777.039, 183015.023, 57612.912, 178438.988, 258448.647, 0,
                           196880.267,
                           219173.528, 39701.25, 190893.9, 17936310.24, 1116831.307, 0, 11891.249, 13114.30292]
    check_argument(
        len(ttj_time_raw) == len(hj_time_raw) and
        len(hj_time_raw) == len(lip_time_raw) and
        len(lip_time_raw) == len(yannakakis_time_raw),
        f"len(ttj_time): {len(ttj_time_raw)}\n"
        f"len(hj_time): {len(hj_time_raw)}\n"
        f"len(lip_time): {len(lip_time_raw)}\n"
        f"len(yannakakis_time): {len(yannakakis_time_raw)}")

    hj_time = convert_time(hj_time_raw, TimeUnits.MINUTES)
    hj_time = [round(dp, 2) for dp in hj_time]
    print(f"hj_time (min): {hj_time}")
    ttj_time = convert_time(ttj_time_raw, TimeUnits.MINUTES)
    ttj_time = [round(dp, 2) for dp in ttj_time]
    print(f"ttj_time (min): {ttj_time}")
    lip_time = convert_time(lip_time_raw, TimeUnits.MINUTES)
    lip_time = [round(dp, 2) for dp in lip_time]
    print(f"lip_time (min): {lip_time}")
    yannakakis_time = convert_time(yannakakis_time_raw, TimeUnits.MINUTES)
    yannakakis_time = [round(dp, 2) for dp in yannakakis_time]
    print(f"yannakakis_time (min): {yannakakis_time}")

    all_data = []
    all_data.extend(hj_time)
    all_data.extend(ttj_time)
    all_data.extend(lip_time)
    all_data.extend(yannakakis_time)
    min_all_data = min(all_data)
    max_all_data = max(all_data)

    hj_time_normalized = normalized_data(hj_time, min_all_data, max_all_data)
    ttj_time_normalized = normalized_data(ttj_time, min_all_data, max_all_data)
    lip_time_normalized = normalized_data(lip_time, min_all_data, max_all_data)
    yannakakis_time_normalized = normalized_data(yannakakis_time, min_all_data, max_all_data)

    # hj_time_log_scale = logscale_data(hj_time_normalized)
    # ttj_time_log_scale = logscale_data(ttj_time_normalized)
    # lip_time_log_scale = logscale_data(lip_time_normalized)
    # yannakakis_time_log_scale = logscale_data(yannakakis_time_normalized)

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots()
    rects_hj = ax.bar(x-width*2, hj_time_normalized, width, label='HJ')
    rects_ttj = ax.bar(x - width, ttj_time_normalized, width, label='TTJ')
    rects_lip = ax.bar(x, lip_time_normalized, width, label='LIP')
    rects_yannakakis = ax.bar(x + width, yannakakis_time_normalized, width, label='Yannakakis')

    # Add some text for labels, title and custom x-axis tick labels, etc.
    ax.set_ylabel('Execution Time (ms)')
    ax.set_title('JOB Queries')
    ax.set_xticks(x, labels)
    ax.legend()
    ax.set_yscale('log')
    # ax.bar_label(rects_hj, padding=3)
    # ax.bar_label(rects_ttj, padding=3)
    # ax.bar_label(rects_lip, padding=3)
    # ax.bar_label(rects_yannakakis, padding=3)

    fig.tight_layout()

    plt.show()


def plot2():
    labels = ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15", "Q16",
              "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q24", "Q25", "Q26", "Q27", "Q28", "Q29", "Q30", "Q31",
              "Q32", "Q33"]
    hj_time_raw = [5692.592, 19770.182, 68953.589, 7038.185, 35680.056, 139277.89, 338242.327, 150411.577, 105522.527,
                   96694.08, 14866.837, 32247.927, 31997.949, 19574.518, 2721267.835, 1811991.514, 1005483.542, 0,
                   1277228.576, 107865.167, 979889.034, 744951.277, 1384755.648, 0, 8004635.372, 129846.219, 113972.074,
                   970952.4, 0, 10466725.68, 0, 2802.248, 4309.092464]
    ttj_time_raw = [5854.4, 21987.404, 73884.01, 7361.372, 39496.423, 149430.538, 61445.287, 127231.712, 98503.482,
                    71501.422, 6755.391, 16800.322, 16845.532, 13143.701, 3448458.578, 1170008.858, 1036009.349, 0,
                    1611318.597, 89627.186, 421262.049, 12681.318, 1883510.001, 0, 52529.775, 144920.125, 136802.21,
                    11804.866, 0, 106051.853, 0, 2802.425, 4634.756037]
    lip_time_raw = [7162.702, 22750.656, 80040.082, 7824.328, 42924.785, 155352.262, 58660.311, 166033.293, 132564.09,
                    85536.746, 7852.384, 34858.254, 35774.06, 23777.129, 2553029.646, 1857042.289, 1015339.206, 0,
                    1506129.121, 126722.885, 363483.812, 717237.824, 1458560.147, 0, 8355419.293, 150769.562,
                    122618.925,
                    1038170.056, 0, 13357413.21, 0, 4499.773, 5847.207379]
    yannakakis_time_raw = [15986.847, 41318.331, 65192.436, 20329.379, 51074.502, 239015.242, 71963.84, 204296.136,
                           173354.361, 220609.033, 22177.698, 29159.732, 28548.85, 17585.668, 266415.782, 884742.599,
                           1179031.569, 216480.1, 385777.039, 183015.023, 57612.912, 178438.988, 258448.647, 0,
                           196880.267,
                           219173.528, 39701.25, 190893.9, 17936310.24, 1116831.307, 0, 11891.249, 13114.30292]
    check_argument(
        len(ttj_time_raw) == len(hj_time_raw) and
        len(hj_time_raw) == len(lip_time_raw) and
        len(lip_time_raw) == len(yannakakis_time_raw),
        f"len(ttj_time): {len(ttj_time_raw)}\n"
        f"len(hj_time): {len(hj_time_raw)}\n"
        f"len(lip_time): {len(lip_time_raw)}\n"
        f"len(yannakakis_time): {len(yannakakis_time_raw)}")

    hj_time = convert_time(hj_time_raw, TimeUnits.MINUTES)
    hj_time = [round(dp, 2) for dp in hj_time]
    print(f"hj_time (min): {hj_time}")
    ttj_time = convert_time(ttj_time_raw, TimeUnits.MINUTES)
    ttj_time = [round(dp, 2) for dp in ttj_time]
    print(f"ttj_time (min): {ttj_time}")
    lip_time = convert_time(lip_time_raw, TimeUnits.MINUTES)
    lip_time = [round(dp, 2) for dp in lip_time]
    print(f"lip_time (min): {lip_time}")
    yannakakis_time = convert_time(yannakakis_time_raw, TimeUnits.MINUTES)
    yannakakis_time = [round(dp, 2) for dp in yannakakis_time]
    print(f"yannakakis_time (min): {yannakakis_time}")

    all_data = []
    all_data.extend(hj_time)
    all_data.extend(ttj_time)
    all_data.extend(lip_time)
    all_data.extend(yannakakis_time)
    min_all_data = find_non_zero_min(all_data)

    bar_min_length = 10
    bar_high_length = 110

    hj_time_bar_length = [min((hj / min_all_data) * bar_min_length, bar_high_length) for hj in hj_time]
    print(f"hj_time_bar_length: {hj_time_bar_length}")
    ttj_time_bar_length = [min((ttj / min_all_data) * bar_min_length, bar_high_length) for ttj in ttj_time]
    print(f"ttj_time_bar_length: {ttj_time_bar_length}")
    lip_time_bar_length = [min((lip / min_all_data) * bar_min_length, bar_high_length) for lip in lip_time]
    print(f"lip_time_bar_length: {lip_time_bar_length}")
    yannakakis_time_bar_length = [min((yannakakis / min_all_data) * bar_min_length, bar_high_length) for yannakakis in
                                  yannakakis_time]
    print(f"yannakakis_time_bar_length: {yannakakis_time_bar_length}")
    check_argument(
        len(hj_time_bar_length) == len(ttj_time_bar_length) and
        len(ttj_time_bar_length) == len(lip_time_bar_length) and
        len(lip_time_bar_length) == len(yannakakis_time_bar_length),
        f"len(ttj_time): {len(ttj_time_bar_length)}\n"
        f"len(hj_time): {len(hj_time_bar_length)}\n"
        f"len(lip_time): {len(lip_time_bar_length)}\n"
        f"len(yannakakis_time): {len(yannakakis_time_bar_length)}")

    print("hj,ttj,lip,yannakakis")
    for i in range(len(hj_time_bar_length)):
        print(
            f"Q{i}: {hj_time_bar_length[i]},{ttj_time_bar_length[i]},{lip_time_bar_length[i]},{yannakakis_time_bar_length[i]}")

    # x = np.arange(len(labels))  # the label locations
    # width = 0.35  # the width of the bars
    #
    # fig, ax = plt.subplots()
    # rects_hj = ax.bar(x, hj_time, width, label='HJ')
    # rects_ttj = ax.bar(x + width*2, ttj_time, width, label='TTJ')
    # rects_lip = ax.bar(x + width*3, lip_time, width, label='LIP')
    # rects_yannakakis = ax.bar(x + width*4, yannakakis_time, width, label='Yannakakis')
    #
    # # Add some text for labels, title and custom x-axis tick labels, etc.
    # ax.set_ylabel('Execution Time (ms)')
    # ax.set_title('JOB Queries')
    # ax.set_xticks(x, labels)
    # ax.legend()
    #
    # # ax.bar_label(rects_hj, padding=3)
    # # ax.bar_label(rects_ttj, padding=3)
    # # ax.bar_label(rects_lip, padding=3)
    # # ax.bar_label(rects_yannakakis, padding=3)
    #
    # fig.tight_layout()
    #
    # plt.show()


if __name__ == "__main__":
    plot()
