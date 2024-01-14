# importing package
import csv
import re

import matplotlib.pyplot as plt
import numpy as np

from plot.utility import check_argument

TTJ_EXECUTION_TIME_BREAKDOWN_SRC = "TTJ Execution Time Breakdown (LIP angle) - TTJ.csv"
HJ_EXECUTION_TIME_BREAKDOWN_SRC = "TTJ Execution Time Breakdown (LIP angle) - Hash Join.csv"


def read_data(algorithm):
    if algorithm == "TTJ":
        filename = TTJ_EXECUTION_TIME_BREAKDOWN_SRC
    else:
        filename = HJ_EXECUTION_TIME_BREAKDOWN_SRC
    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        queries, runtime, build_no_good_list, no_good_probe, delete_tuple_from_H, probe_hash_table, hash_table_build, materialization = \
            [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
        for row in csv_reader:
            queries.append(row['Query'])
            runtime = np.append(runtime, float(row['Runtime']))
            build_no_good_list = np.append(build_no_good_list, float(row['Build No-Good List ']))
            no_good_probe = np.append(no_good_probe, float(row['No-good List Probing Time']))
            delete_tuple_from_H = np.append(delete_tuple_from_H, float(row['Delete Tuple From H']))
            probe_hash_table = np.append(probe_hash_table, float(row['ProbeHashTable']))
            hash_table_build = np.append(hash_table_build, float(row['HashTableBuildTime']))
            materialization = np.append(materialization, float(row['Materialization']))
    return queries, runtime, build_no_good_list, no_good_probe, delete_tuple_from_H, probe_hash_table, hash_table_build, materialization


def draw(algorithm):
    queries, runtime, build_no_good_list, no_good_probe, delete_tuple_from_H, probe_hash_table, hash_table_build, materialization\
        = read_data(algorithm)

    # truncate queries name
    truncate_queries = []
    for query in queries:
        replaced = re.sub('JoinTreeOptOrdering', '', query)
        truncate_queries.append(replaced)

    # plot bars in stack manner
    plt.bar(truncate_queries, materialization)
    plt.bar(truncate_queries, build_no_good_list, bottom=materialization)
    plt.bar(truncate_queries, no_good_probe, bottom=materialization + build_no_good_list)
    plt.bar(truncate_queries, delete_tuple_from_H, bottom=materialization + build_no_good_list + no_good_probe)
    plt.bar(truncate_queries, probe_hash_table, bottom=materialization + build_no_good_list + no_good_probe + delete_tuple_from_H)
    plt.bar(truncate_queries, hash_table_build, bottom=materialization + build_no_good_list + no_good_probe + delete_tuple_from_H + probe_hash_table)
    plt.xlabel("Queries")
    plt.ylabel("Runtime (ms)")
    plt.legend(["Materialization", "Build No-Good List", "No-Good Probe", "Delete Dangling Tuple from H", "Probe Hash Table", "Hash Table Build"])
    plt.title(f"Execution Time Breakdown ({algorithm})")
    plt.xticks(rotation=45, ha='right')
    plt.show()


def draw2(ordering):
    """
    Fix ordering, compare TTJ and HJ
    """

    def filter_queries(query, ordering):
        if ordering == "draft":
            if "Opt" in query:
                return False
            return True
        if ordering == "opt":
            if "Opt" in query:
                return True
            return False

    ttj_queries, ttj_runtime, ttj_build_no_good_list, ttj_no_good_probe, ttj_delete_tuple_from_H, ttj_probe_hash_table, \
        ttj_hash_table_build, ttj_materialization = read_data("TTJ")
    hj_queries, hj_runtime, hj_build_no_good_list, hj_no_good_probe, hj_delete_tuple_from_H, hj_probe_hash_table, \
        hj_hash_table_build, hj_materialization = read_data("HJ")
    check_argument(ttj_queries == hj_queries, f"ttj_queries {ttj_queries} doesn't equal to hj_queries {hj_queries}")
    ttj_target_queries, ttj_target_build_no_good_list, ttj_target_no_good_probe, ttj_target_delete_tuple_from_H, \
        ttj_target_probe_hash_table, ttj_target_hash_table_build, ttj_target_materialization = \
        [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
    hj_target_queries, hj_target_build_no_good_list, hj_target_no_good_probe, hj_target_delete_tuple_from_H, \
        hj_target_probe_hash_table, hj_target_hash_table_build, hj_target_materialization = \
        [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
    for i, query in enumerate(ttj_queries):
        if filter_queries(query, ordering):
            ttj_target_queries.append(query)
            hj_target_queries.append(query)
            ttj_target_build_no_good_list = np.append(ttj_target_build_no_good_list, ttj_build_no_good_list[i])
            hj_target_build_no_good_list = np.append(hj_target_build_no_good_list, hj_build_no_good_list[i])
            ttj_target_no_good_probe = np.append(ttj_target_no_good_probe, ttj_no_good_probe[i])
            hj_target_no_good_probe = np.append(hj_target_no_good_probe, hj_no_good_probe[i])
            ttj_target_delete_tuple_from_H = np.append(ttj_target_delete_tuple_from_H, ttj_delete_tuple_from_H[i])
            hj_target_delete_tuple_from_H = np.append(hj_target_delete_tuple_from_H, hj_delete_tuple_from_H[i])
            ttj_target_probe_hash_table = np.append(ttj_target_probe_hash_table, ttj_probe_hash_table[i])
            hj_target_probe_hash_table = np.append(hj_target_probe_hash_table, hj_probe_hash_table[i])
            ttj_target_hash_table_build = np.append(ttj_target_hash_table_build, ttj_hash_table_build[i])
            hj_target_hash_table_build = np.append(hj_target_hash_table_build, hj_hash_table_build[i])
            ttj_target_materialization = np.append(ttj_target_materialization, ttj_materialization[i])
            hj_target_materialization = np.append(hj_target_materialization, hj_materialization[i])

    if ordering == "opt":
        # truncate queries name
        truncate_queries = []
        for query in ttj_target_queries:
            replaced = re.sub('JoinTreeOptOrdering', '', query)
            truncate_queries.append(replaced)
    else:
        truncate_queries = ttj_target_queries

    # set width of bar
    barWidth = 0.25
    fig = plt.subplots(figsize=(12, 8))

    # Set position of bar on X axis
    br1 = np.arange(len(truncate_queries))
    br2 = [x + barWidth for x in br1]

    # Make the plot
    plt.bar(br1, ttj_target_materialization, color='tab:blue', width=barWidth, label='Materialization')
    plt.bar(br1, ttj_target_build_no_good_list, color='tab:orange', bottom=ttj_target_materialization, width=barWidth,
            label='Build No-Good List')
    plt.bar(br1, ttj_target_no_good_probe, color='tab:green', bottom=ttj_target_materialization + ttj_target_build_no_good_list,
            width=barWidth, label='No-Good Probe')
    plt.bar(br1, ttj_target_delete_tuple_from_H, color='tab:red',
            bottom=ttj_target_materialization + ttj_target_build_no_good_list + ttj_target_no_good_probe, width=barWidth,
            label='Delete Dangling Tuple from H')
    plt.bar(br1, hj_target_probe_hash_table, color='tab:purple',
            bottom=ttj_target_materialization + ttj_target_build_no_good_list + ttj_target_no_good_probe + ttj_target_delete_tuple_from_H,
            width=barWidth, label='Probe Hash Table')
    plt.bar(br1, ttj_target_hash_table_build, color='tab:brown',
            bottom=ttj_target_materialization + ttj_target_build_no_good_list + ttj_target_no_good_probe + ttj_target_delete_tuple_from_H + hj_target_probe_hash_table,
            width=barWidth, label='Hash Table Build')
    for x, y in zip(br1,
                    ttj_target_hash_table_build + ttj_target_materialization + ttj_target_build_no_good_list + ttj_target_no_good_probe + ttj_target_delete_tuple_from_H + hj_target_probe_hash_table):
        plt.text(x - barWidth / 2.0, y, 'TTJ')

    plt.bar(br2, hj_target_materialization, color='tab:blue', width=barWidth)
    plt.bar(br2, hj_target_build_no_good_list, color='tab:orange', bottom=hj_target_materialization, width=barWidth)
    plt.bar(br2, hj_target_no_good_probe, color='tab:green', bottom=hj_target_materialization + hj_target_build_no_good_list,
            width=barWidth)
    plt.bar(br2, hj_target_delete_tuple_from_H, color='tab:red',
            bottom=hj_target_materialization + hj_target_build_no_good_list + hj_target_no_good_probe, width=barWidth)
    plt.bar(br2, hj_target_probe_hash_table, color='tab:purple',
            bottom=hj_target_materialization + hj_target_build_no_good_list + hj_target_no_good_probe + hj_target_delete_tuple_from_H,
            width=barWidth)
    plt.bar(br2, hj_target_hash_table_build, color='tab:brown',
            bottom=hj_target_materialization + hj_target_build_no_good_list + hj_target_no_good_probe + hj_target_delete_tuple_from_H + hj_target_probe_hash_table,
            width=barWidth)
    for x, y in zip(br1,
                    hj_target_hash_table_build + hj_target_materialization + hj_target_build_no_good_list + hj_target_no_good_probe + hj_target_delete_tuple_from_H + hj_target_probe_hash_table):
        plt.text(x + barWidth / 2.0, y, 'HJ')

    # Adding Xticks
    plt.xlabel('Queries', fontweight='bold', fontsize=15)
    plt.ylabel('Runtime (ms)', fontweight='bold', fontsize=15)
    plt.xticks([r + barWidth for r in range(len(truncate_queries))],
               truncate_queries)
    if ordering == 'draft':
        ordering_name = 'Draft Ordering'
    else:
        ordering_name = 'Opt Ordering'
    plt.title(f"Execution Time Breakdown Comparison ({ordering_name})")
    plt.legend()
    plt.show()

if __name__ == "__main__":
    # draw("TTJ")
    # draw("HJ")
    # draw2("draft")
    draw2("opt")