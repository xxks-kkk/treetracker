"""
Experiment: Total intermediate results produced

The scatter plot with x-axis: input size after evaluation; y-axis: intermediate result size produced
"""
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from plot.constants import DATA_SOURCE_CSV, COLUMN_RIGHT_BOUND, ALGORITHMS_TO_PLOT, HJ, TTJ, Yannakakis, LIP
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument, to_float

input_size_after_evaluation = "input_size_after_evaluation"
total_intermediate_results_produced = "total_intermediate_results_produced"

def get_ssb_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "simple-cost-model-with-predicates" / csv_name


def extract_data_from_agg_csv(DATA_SOURCE_CSV, get_full_path_func):
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
                if "totalIntermediateResultsProduced" in row:
                    result[algorithm][total_intermediate_results_produced] = [to_float(num) for num in row[1:]]
                elif "totalInputSizeAfterEvaluation" in row:
                    result[algorithm][input_size_after_evaluation] = [to_float(num) for num in row[1:]]
    return result

def plot_ssb():
    conf = {
        DATA_SOURCE_CSV: {TTJ: "TTJHP_SSB_aggregagateStatistics.csv",
                          HJ: "HASH_JOIN_SSB_aggregagateStatistics.csv",
                          Yannakakis : "Yannakakis_SSB_aggregagateStatistics.csv",
                          LIP : "LIP_SSB_aggregagateStatistics.csv"}
    }
    prem_data = extract_data_from_agg_csv(conf[DATA_SOURCE_CSV], get_ssb_full_path)
    hj_input_size = prem_data[HJ][input_size_after_evaluation]
    hj_intermediate = prem_data[HJ][total_intermediate_results_produced]
    ttj_input_size = prem_data[TTJ][input_size_after_evaluation]
    ttj_intermediate = prem_data[TTJ][total_intermediate_results_produced]
    lip_input_size = prem_data[LIP][input_size_after_evaluation]
    lip_intermediate = prem_data[LIP][total_intermediate_results_produced]
    yannakakis_input_size = prem_data[Yannakakis][input_size_after_evaluation]
    yannakakis_intermediate = prem_data[Yannakakis][total_intermediate_results_produced]

    f, ax = plt.subplots(figsize=(6, 6))

    ax.scatter(hj_input_size, hj_intermediate, s=5, label = 'HJ')
    ax.scatter(ttj_input_size, ttj_intermediate, s=5, label = 'TTJ')
    ax.scatter(lip_input_size, lip_intermediate, s=5, label= 'LIP')
    ax.scatter(yannakakis_input_size, yannakakis_intermediate, s=5, label='Yannakakis')

    # inset axes....
    axins = ax.inset_axes(
        [0.2, 0.2, 0.7, 0.6],
        xlim=(0, 800000), ylim=(0, 900000), xticklabels=[], yticklabels=[], xticks=[], yticks=[])
    axins.scatter(hj_input_size, hj_intermediate, s=5, label = 'HJ')
    axins.scatter(ttj_input_size, ttj_intermediate, s=5, label = 'TTJ')
    axins.scatter(lip_input_size, lip_intermediate, s=5, label= 'LIP')
    axins.scatter(yannakakis_input_size, yannakakis_intermediate, s=5, label='Yannakakis')
    ax.indicate_inset_zoom(axins, edgecolor="black")

    # Set the axis labels
    plt.xlabel("input size after evaluation")
    plt.ylabel("intermediate results produced")
    plt.margins(x=0,y=0)
    # Add a grid
    # plt.grid(True)
    plt.legend(loc='best')

    plt.tight_layout()
    plt.savefig("exp2.2-scatter.pdf", format='pdf')
    # Show the plot
    plt.show()

if __name__ == "__main__":
    plot_ssb()