from matplotlib import pyplot as plt

from plot_execution_time_job import convert_time, TimeUnits


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

    Credits: https://stackoverflow.com/a/60270421/1460102
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

if __name__ == "__main__":
    # Usage example:
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
                1506129.121, 126722.885, 363483.812, 717237.824, 1458560.147, 0, 8355419.293, 150769.562, 122618.925,
                1038170.056, 0, 13357413.21, 0, 4499.773, 5847.207379]
    yannakakis_time_raw = [15986.847, 41318.331, 65192.436, 20329.379, 51074.502, 239015.242, 71963.84, 204296.136,
                       173354.361, 220609.033, 22177.698, 29159.732, 28548.85, 17585.668, 266415.782, 884742.599,
                       1179031.569, 216480.1, 385777.039, 183015.023, 57612.912, 178438.988, 258448.647, 0, 196880.267,
                       219173.528, 39701.25, 190893.9, 17936310.24, 1116831.307, 0, 11891.249, 13114.30292]
    hj_time = convert_time(hj_time_raw, TimeUnits.MINUTES)
    ttj_time = convert_time(ttj_time_raw, TimeUnits.MINUTES)
    lip_time = convert_time(lip_time_raw, TimeUnits.MINUTES)
    yannakakis_time = convert_time(yannakakis_time_raw, TimeUnits.MINUTES)
    data = {
        # "a": [1, 2, 3, 2, 1],
        # "b": [2, 3, 4, 3, 1],
        # "c": [3, 2, 1, 4, 2],
        # "d": [5, 9, 2, 1, 8],
        # "e": [1, 3, 2, 2, 3],
        # "f": [4, 3, 1, 1, 4],
        "hj": hj_time,
        "ttj": ttj_time,
        "lip": lip_time,
        "yannakakis": yannakakis_time
    }

    fig, ax = plt.subplots()
    bar_plot(ax, data, total_width=.8, single_width=.9)
    plt.show()
