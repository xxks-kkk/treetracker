"""
TODS revision plots (plot TTJ-eps against other algorithms)
"""
import logging
import pandas as pd
from pathlib import Path

from matplotlib import pyplot as plt

logging.getLogger('matplotlib.font_manager').disabled = True

# Algorithm constants
TTJ_EPS = "TTJ-eps"
DUCKDB = "DuckDB"
YA_PLUS = "YA+"
RPT = "RPT"
TTJ = "TTJ"


def load_data_from_csv():
    """Load data from rpt-sheet7.csv"""
    csv_path = Path(__file__).parent / "rpt-sheet7.csv"
    df = pd.read_csv(csv_path)
    return df


class AlgorithmPair:
    def __init__(self, y_axis_algorithm, x_axis_algorithm, show_y_axis=True):
        self.x_axis_algorithm = x_axis_algorithm
        self.y_axis_algorithm = y_axis_algorithm
        self.show_y_axis = show_y_axis


def plot(algorithm_pair):
    # Load data from CSV
    df = load_data_from_csv()
    
    # Extract time data for x and y axes
    x_axis_times = df[algorithm_pair.x_axis_algorithm].tolist()
    y_axis_times = df[algorithm_pair.y_axis_algorithm].tolist()
    
    # Create a scatter plot
    f, ax = plt.subplots(figsize=(6, 6))
    ax.axline((1, 1), slope=1, color='#5b5b5b', linewidth=0.2)

    ax.set_yscale('log')
    ax.set_xscale('log')
    ax.set_aspect('equal', adjustable='box')

    axis_label_font_size = 25

    # Move y-axis tick marks to the right side of the y-axis (inside the plot)
    ax.tick_params(axis='y', direction='in', pad=10, labelsize=20)
    ax.tick_params(axis='x', direction='in', pad=10, labelsize=20)



    # Set up plot based on algorithm pair
    if algorithm_pair.x_axis_algorithm == DUCKDB and algorithm_pair.y_axis_algorithm == TTJ_EPS:
        plt.xlim(0.001, 10)
        plt.ylim(0.001, 10)
        plt.xlabel(r"$\mathsf{DuckDB}$ time (s)", fontsize=axis_label_font_size)
        if algorithm_pair.show_y_axis:
            plt.ylabel(r"$\mathsf{TTJ^\varepsilon}$ time (s)", fontsize=axis_label_font_size)
        else:
            plt.ylabel("")
        plt.scatter(x_axis_times, y_axis_times, s=20, alpha=0.7, color='black')
        
    elif algorithm_pair.x_axis_algorithm == YA_PLUS and algorithm_pair.y_axis_algorithm == TTJ_EPS:
        plt.xlim(0.001, 10)
        plt.ylim(0.001, 10)
        plt.xlabel(r"$\mathsf{YA}^+$ time (s)", fontsize=axis_label_font_size)
        if algorithm_pair.show_y_axis:
            plt.ylabel(r"$\mathsf{TTJ^\varepsilon}$ time (s)", fontsize=axis_label_font_size)
        else:
            plt.ylabel("")
        plt.scatter(x_axis_times, y_axis_times, s=20, alpha=0.7, color='black')
        
    elif algorithm_pair.x_axis_algorithm == RPT and algorithm_pair.y_axis_algorithm == TTJ_EPS:
        plt.xlim(0.001, 10)
        plt.ylim(0.001, 10)
        plt.xlabel(r"$\mathsf{RPT}$ time (s)", fontsize=axis_label_font_size)
        if algorithm_pair.show_y_axis:
            plt.ylabel(r"$\mathsf{TTJ^\varepsilon}$ time (s)", fontsize=axis_label_font_size)
        else:
            plt.ylabel("")
        plt.scatter(x_axis_times, y_axis_times, s=20, alpha=0.7, color='black')
        
    elif algorithm_pair.x_axis_algorithm == TTJ_EPS and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.001, 10)
        plt.ylim(0.001, 10)
        plt.xlabel(r"$\mathsf{TTJ^\varepsilon}$ time (s)", fontsize=axis_label_font_size)
        if algorithm_pair.show_y_axis:
            plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        else:
            plt.ylabel("")
        plt.scatter(x_axis_times, y_axis_times, s=20, alpha=0.7, color='black')
    elif algorithm_pair.x_axis_algorithm == RPT and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.001, 10)
        plt.ylim(0.001, 10)
        plt.xlabel(r"$\mathsf{RPT}$ time (s)", fontsize=axis_label_font_size)
        if algorithm_pair.show_y_axis:
            plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        else:
            plt.ylabel("")
        plt.scatter(x_axis_times, y_axis_times, s=20, alpha=0.7, color='black')
    elif algorithm_pair.x_axis_algorithm == YA_PLUS and algorithm_pair.y_axis_algorithm == TTJ:
        plt.xlim(0.001, 10)
        plt.ylim(0.001, 10)
        plt.xlabel(r"$\mathsf{YA}^+$ time (s)", fontsize=axis_label_font_size)
        if algorithm_pair.show_y_axis:
            plt.ylabel(r"$\mathsf{TTJ}$ time (s)", fontsize=axis_label_font_size)
        else:
            plt.ylabel("")
        plt.scatter(x_axis_times, y_axis_times, s=20, alpha=0.7, color='black')

    plt.tight_layout()
    
    # Save figure based on algorithm pair
    if algorithm_pair.x_axis_algorithm == DUCKDB and algorithm_pair.y_axis_algorithm == TTJ_EPS:
        plt.savefig("ttj-eps-duckdb.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == YA_PLUS and algorithm_pair.y_axis_algorithm == TTJ_EPS:
        plt.savefig("ttj-eps-ya-plus.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == RPT and algorithm_pair.y_axis_algorithm == TTJ_EPS:
        plt.savefig("ttj-eps-rpt.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == TTJ_EPS and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("ttj-eps-ttj.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == RPT and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("ttj-rpt.pdf", format='pdf')
    elif algorithm_pair.x_axis_algorithm == YA_PLUS and algorithm_pair.y_axis_algorithm == TTJ:
        plt.savefig("ttj-ya-plus.pdf", format='pdf')
    
    plt.show()


if __name__ == '__main__':
    # Create four plots: TTJ-eps vs. DuckDB, TTJ-eps vs. YA+, TTJ-eps vs. RPT, and TTJ-eps vs. TTJ
    # Only the first plot (DuckDB) shows y-axis labels and numbers
    plot(AlgorithmPair(TTJ_EPS, DUCKDB, show_y_axis=True))
    plot(AlgorithmPair(TTJ_EPS, YA_PLUS, show_y_axis=True))
    plot(AlgorithmPair(TTJ_EPS, RPT, show_y_axis=True))
    plot(AlgorithmPair(TTJ, TTJ_EPS, show_y_axis=True))
    plot(AlgorithmPair(TTJ, RPT, show_y_axis=True))
    plot(AlgorithmPair(TTJ, YA_PLUS, show_y_axis=True))
