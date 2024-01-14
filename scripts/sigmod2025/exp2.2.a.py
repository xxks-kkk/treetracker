"""
Experiment: Total intermediate results produced

stacked bar plot
"""
import csv
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from plot.constants import DATA_SOURCE_CSV, HJ, TTJ, Yannakakis, LIP
from plot.utility import to_float

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

    # set width of bar
    barWidth = 0.25
    fig = plt.subplots(figsize=(12, 8))
    labels = ["1.1", "1.2", "1.3", "2.1", "2.2", "2.3", "3.1", "3.2", "3.3", "3.4", "4.1", "4.2", "4.3"]
    # Set position of bar on X axis
    br1 = np.arange(len(labels))
    br2 = [x + barWidth for x in br1]
    br3 = [x + barWidth for x in br2]
    br4 = [x + barWidth for x in br3]

    # Make the plot
    plt.bar(br1, ttj_input_size, color='tab:blue', width=barWidth, label=input_size_after_evaluation)
    plt.bar(br1, ttj_intermediate, color='tab:olive', bottom=ttj_input_size, width=barWidth, label=total_intermediate_results_produced)
    for x, y in zip(br1, ttj_input_size + ttj_intermediate):
        plt.text(x - barWidth / 2.0, y, 'TTJ')

    plt.bar(br2, hj_input_size, color='tab:blue', width=barWidth)
    plt.bar(br2, hj_intermediate, color='tab:olive', bottom=hj_input_size, width=barWidth)
    for x, y in zip(br2, hj_input_size + hj_intermediate):
        plt.text(x + barWidth / 2.0, y, 'HJ')

    plt.bar(br3, yannakakis_input_size, color='tab:blue', width=barWidth)
    plt.bar(br3, yannakakis_intermediate, color='tab:olive', bottom=yannakakis_input_size, width=barWidth)
    for x, y in zip(br3, yannakakis_input_size + yannakakis_intermediate):
        plt.text(x + barWidth / 2.0, y, 'Yannakakis')

    plt.bar(br4, lip_input_size, color='tab:blue', width=barWidth)
    plt.bar(br4, lip_intermediate, color='tab:olive', bottom=lip_input_size, width=barWidth)
    for x, y in zip(br4, lip_input_size + lip_intermediate):
        plt.text(x + barWidth / 2.0, y, 'LIP')

    # Adding Xticks
    plt.xlabel('count', fontweight='bold', fontsize=15)
    plt.ylabel('queries', fontweight='bold', fontsize=15)
    plt.xticks([r + barWidth for r in range(len(labels))],
               labels)
    plt.legend()
    plt.show()

if __name__ == "__main__":
    plot_ssb()