import csv
import re

import numpy as np
from matplotlib import pyplot as plt

from plot.utility import check_argument

TTJ_EXECUTION_TIME_BREAKDOWN_SRC = "Microbenchmark Queries Execution Time breakdown - TTJ.csv"
HJ_EXECUTION_TIME_BREAKDOWN_SRC = "Microbenchmark Queries Execution Time breakdown - Hash Join.csv"
TTJ_NO_NG_EXECUTION_TIME_BREAKDOWN_SRC = "Microbenchmark Queries Execution Time breakdown - TTJ_NO_NG.csv"


def read_data(algorithm):
    if algorithm == "TTJ":
        filename = TTJ_EXECUTION_TIME_BREAKDOWN_SRC
    elif algorithm == "TTJHP_NO_NG":
        filename = TTJ_NO_NG_EXECUTION_TIME_BREAKDOWN_SRC
    else:
        filename = HJ_EXECUTION_TIME_BREAKDOWN_SRC
    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        queries, runtime, tuple_fetch, no_good_probe, pass_context, join_time, hash_table_build, other = \
            [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
        for row in csv_reader:
            queries.append(row['Query'])
            runtime = np.append(runtime, float(row['Runtime']))
            tuple_fetch = np.append(tuple_fetch, float(row['Total Tuple Fetching Time']))
            no_good_probe = np.append(no_good_probe, float(row['No-good List Probing Time']))
            pass_context = np.append(pass_context, float(row['PassContextWorkTime']))
            join_time = np.append(join_time, float(row['JoinTime']))
            hash_table_build = np.append(hash_table_build, float(row['HashTableBuildTime']))
            other = np.append(other, float(row['Other']))
    return queries, runtime, tuple_fetch, no_good_probe, pass_context, join_time, hash_table_build, other


def draw2():
    """
    compare TTJHP, TTJHP_NO_NG, HJ
    """
    ttj_queries, ttj_runtime, ttj_tuple_fetch, \
        ttj_no_good_probe, ttj_pass_context, ttj_join_time, ttj_hash_table_build, ttj_other = read_data("TTJ")
    hj_queries, hj_runtime, hj_tuple_fetch, \
        hj_no_good_probe, hj_pass_context, hj_join_time, hj_hash_table_build, hj_other = read_data("HJ")
    ttj_no_ng_queries, ttj_no_ng_runtime, ttj_no_ng_tuple_fetch, \
        ttj_no_ng_no_good_probe, ttj_no_ng_pass_context, ttj_no_ng_join_time, ttj_no_ng_hash_table_build, ttj_no_ng_other = read_data("TTJHP_NO_NG")

    check_argument(ttj_queries == hj_queries, f"ttj_queries {ttj_queries} doesn't equal to hj_queries {hj_queries}")
    check_argument(ttj_no_ng_queries == hj_queries, f"ttj_no_ng_queries {ttj_queries} doesn't equal to hj_queries {hj_queries}")

    ttj_target_queries, ttj_target_tuple_fetch, ttj_target_no_good_probe, ttj_target_pass_context, \
        ttj_target_join_time, ttj_target_hash_table_build, ttj_target_other = \
        [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
    hj_target_queries, hj_target_tuple_fetch, hj_target_no_good_probe, hj_target_pass_context, \
        hj_target_join_time, hj_target_hash_table_build, hj_target_other = \
        [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
    ttj_no_ng_target_queries, ttj_no_ng_target_tuple_fetch, ttj_no_ng_target_no_good_probe, ttj_no_ng_target_pass_context, \
        ttj_no_ng_target_join_time, ttj_no_ng_target_hash_table_build, ttj_no_ng_target_other = \
        [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])


    for i, query in enumerate(ttj_queries):
        ttj_target_queries.append(query)
        hj_target_queries.append(query)
        ttj_no_ng_target_queries.append(query)

        ttj_target_tuple_fetch = np.append(ttj_target_tuple_fetch, ttj_tuple_fetch[i])
        hj_target_tuple_fetch = np.append(hj_target_tuple_fetch, hj_tuple_fetch[i])
        ttj_no_ng_target_tuple_fetch = np.append(ttj_no_ng_target_tuple_fetch, ttj_no_ng_tuple_fetch[i])

        ttj_target_no_good_probe = np.append(ttj_target_no_good_probe, ttj_no_good_probe[i])
        hj_target_no_good_probe = np.append(hj_target_no_good_probe, hj_no_good_probe[i])
        ttj_no_ng_target_no_good_probe = np.append(ttj_no_ng_target_no_good_probe, ttj_no_good_probe[i])

        ttj_target_pass_context = np.append(ttj_target_pass_context, ttj_pass_context[i])
        hj_target_pass_context = np.append(hj_target_pass_context, hj_pass_context[i])
        ttj_no_ng_target_pass_context = np.append(ttj_no_ng_target_pass_context, ttj_no_ng_pass_context[i])

        ttj_target_join_time = np.append(ttj_target_join_time, ttj_join_time[i])
        hj_target_join_time = np.append(hj_target_join_time, hj_join_time[i])
        ttj_no_ng_target_join_time = np.append(ttj_no_ng_target_join_time, ttj_no_ng_join_time[i])

        ttj_target_hash_table_build = np.append(ttj_target_hash_table_build, ttj_hash_table_build[i])
        hj_target_hash_table_build = np.append(hj_target_hash_table_build, hj_hash_table_build[i])
        ttj_no_ng_target_hash_table_build = np.append(ttj_no_ng_target_hash_table_build, ttj_no_ng_hash_table_build[i])

        ttj_target_other = np.append(ttj_target_other, ttj_other[i])
        hj_target_other = np.append(hj_target_other, hj_other[i])
        ttj_no_ng_target_other = np.append(ttj_no_ng_target_other, ttj_no_ng_other[i])

    truncate_queries = ttj_target_queries

    # set width of bar
    barWidth = 0.25
    fig = plt.subplots(figsize=(12, 8))

    # Set position of bar on X axis
    br1 = np.arange(len(truncate_queries))
    br2 = [x + barWidth for x in br1]
    br3 = [x + barWidth for x in br2]

    # Make the plot
    plt.bar(br1, ttj_target_other, color='tab:blue', width=barWidth, label='Other')
    plt.bar(br1, ttj_target_tuple_fetch, color='tab:orange', bottom=ttj_target_other, width=barWidth,
            label='Tuple Fetch')
    plt.bar(br1, ttj_target_no_good_probe, color='tab:green', bottom=ttj_target_other + ttj_target_tuple_fetch,
            width=barWidth, label='No-Good Probe')
    plt.bar(br1, ttj_target_pass_context, color='tab:red',
            bottom=ttj_target_other + ttj_target_tuple_fetch + ttj_target_no_good_probe, width=barWidth,
            label='PassContext')
    plt.bar(br1, ttj_target_join_time, color='tab:purple',
            bottom=ttj_target_other + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_pass_context,
            width=barWidth, label='Join')
    plt.bar(br1, ttj_target_hash_table_build, color='tab:brown',
            bottom=ttj_target_other + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_pass_context + ttj_target_join_time,
            width=barWidth, label='Hash Table Build')
    for x, y in zip(br1,
                    ttj_target_hash_table_build + ttj_target_other + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_pass_context + ttj_target_join_time):
        plt.text(x - barWidth / 2.0, y, 'TTJ')

    plt.bar(br2, hj_target_other, color='tab:blue', width=barWidth)
    plt.bar(br2, hj_target_tuple_fetch, color='tab:orange', bottom=hj_target_other, width=barWidth)
    plt.bar(br2, hj_target_no_good_probe, color='tab:green', bottom=hj_target_other + hj_target_tuple_fetch,
            width=barWidth)
    plt.bar(br2, hj_target_pass_context, color='tab:red',
            bottom=hj_target_other + hj_target_tuple_fetch + hj_target_no_good_probe, width=barWidth)
    plt.bar(br2, hj_target_join_time, color='tab:purple',
            bottom=hj_target_other + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_pass_context,
            width=barWidth)
    plt.bar(br2, hj_target_hash_table_build, color='tab:brown',
            bottom=hj_target_other + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_pass_context + hj_target_join_time,
            width=barWidth)
    for x, y in zip(br1,
                    hj_target_hash_table_build + hj_target_other + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_pass_context + hj_target_join_time):
        plt.text(x + barWidth / 2.0, y, 'HJ')

    plt.bar(br3, ttj_no_ng_target_other, color='tab:blue', width=barWidth)
    plt.bar(br3, ttj_no_ng_target_tuple_fetch, color='tab:orange', bottom=ttj_no_ng_target_other, width=barWidth)
    plt.bar(br3, ttj_no_ng_target_no_good_probe, color='tab:green', bottom=ttj_no_ng_target_other + ttj_no_ng_target_tuple_fetch,
            width=barWidth)
    plt.bar(br3, ttj_no_ng_target_pass_context, color='tab:red',
            bottom=ttj_no_ng_target_other + ttj_no_ng_target_tuple_fetch + ttj_no_ng_target_no_good_probe, width=barWidth)
    plt.bar(br3, ttj_no_ng_target_join_time, color='tab:purple',
            bottom=ttj_no_ng_target_other + ttj_no_ng_target_tuple_fetch + ttj_no_ng_target_no_good_probe + ttj_no_ng_target_pass_context,
            width=barWidth)
    plt.bar(br3, ttj_no_ng_target_hash_table_build, color='tab:brown',
            bottom=ttj_no_ng_target_other + ttj_no_ng_target_tuple_fetch + ttj_no_ng_target_no_good_probe + ttj_no_ng_target_pass_context + ttj_no_ng_target_join_time,
            width=barWidth)
    for x, y in zip(br3,
                    ttj_no_ng_target_hash_table_build + ttj_no_ng_target_other + ttj_no_ng_target_tuple_fetch + ttj_no_ng_target_no_good_probe + ttj_no_ng_target_pass_context + ttj_no_ng_target_join_time):
        plt.text(x - barWidth / 2.0, y, 'TTJ_NO_NG')

    # Adding Xticks
    plt.xlabel('Queries', fontweight='bold', fontsize=15)
    plt.ylabel('Runtime (ms)', fontweight='bold', fontsize=15)
    plt.xticks([r + barWidth for r in range(len(truncate_queries))],
               truncate_queries)
    plt.title(f"Execution Time Breakdown Comparison")
    plt.legend()
    plt.show()

if __name__ == "__main__":
    draw2()
