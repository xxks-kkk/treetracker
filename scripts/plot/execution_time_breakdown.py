# importing package
import csv
import re

import matplotlib.pyplot as plt
import numpy as np

from plot.utility import check_argument

# TTJ_EXECUTION_TIME_BREAKDOWN_SRC = "TTJ Execution Time Breakdown - TTJ.csv"
# HJ_EXECUTION_TIME_BREAKDOWN_SRC = "TTJ Execution Time Breakdown - Hash Join.csv"

TTJ_EXECUTION_TIME_BREAKDOWN_SRC = "ttj.csv"
HJ_EXECUTION_TIME_BREAKDOWN_SRC = "hash_join.csv"

def read_data(algorithm):
    if algorithm == "TTJ":
        filename = TTJ_EXECUTION_TIME_BREAKDOWN_SRC
    else:
        filename = HJ_EXECUTION_TIME_BREAKDOWN_SRC
    with open(filename, mode='r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        queries, runtime, tuple_fetch, no_good_probe, no_good_construct, pass_context, join_time, hash_table_build, predicate_evaluation, other = \
            [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
        for row in csv_reader:
            queries.append(row['Query'])
            runtime = np.append(runtime, float(row['Runtime']))
            tuple_fetch = np.append(tuple_fetch, float(row['Total Tuple Fetching Time']))
            no_good_probe = np.append(no_good_probe, float(row['No-good List Probing Time']))
            no_good_construct = np.append(no_good_construct, float(row["No-good List Construct Time"]))
            pass_context = np.append(pass_context, float(row['PassContextWorkTime']))
            join_time = np.append(join_time, float(row['JoinTime']))
            hash_table_build = np.append(hash_table_build, float(row['HashTableBuildTime']))
            predicate_evaluation = np.append(predicate_evaluation, float(row['Predicate Evaluation']))
            other = np.append(other, float(row['Other']))
    return queries, runtime, tuple_fetch, no_good_probe, no_good_construct, pass_context, join_time, hash_table_build, \
        predicate_evaluation, other


# def draw(algorithm):
#     queries, runtime, tuple_fetch, no_good_probe, pass_context, join_time, hash_table_build, other = read_data(
#         algorithm)
#
#     # truncate queries name
#     truncate_queries = []
#     for query in queries:
#         replaced = re.sub('JoinTreeOptOrdering', '', query)
#         truncate_queries.append(replaced)
#
#     # plot bars in stack manner
#     plt.bar(truncate_queries, other)
#     plt.bar(truncate_queries, tuple_fetch, bottom=other)
#     plt.bar(truncate_queries, no_good_probe, bottom=other + tuple_fetch)
#     plt.bar(truncate_queries, pass_context, bottom=other + tuple_fetch + no_good_probe)
#     plt.bar(truncate_queries, join_time, bottom=other + tuple_fetch + no_good_probe + pass_context)
#     plt.bar(truncate_queries, hash_table_build, bottom=other + tuple_fetch + no_good_probe + pass_context + join_time)
#     plt.xlabel("Queries")
#     plt.ylabel("Runtime (ms)")
#     plt.legend(["Other", "Tuple Fetch", "No-Good Probe", "PassContext", "Join", "Hash Table Build"])
#     plt.title(f"Execution Time Breakdown ({algorithm})")
#     plt.xticks(rotation=45, ha='right')
#     plt.show()


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

    ttj_queries, ttj_runtime, ttj_tuple_fetch, \
        ttj_no_good_probe, ttj_no_good_construct, ttj_pass_context, ttj_join_time, ttj_hash_table_build, ttj_predicate_evaluation, \
        ttj_other = read_data("TTJ")
    hj_queries, hj_runtime, hj_tuple_fetch, \
        hj_no_good_probe, hj_no_good_construct, hj_pass_context, hj_join_time, hj_hash_table_build, hj_predicate_evaluation, \
        hj_other = read_data("HJ")
    check_argument(ttj_queries == hj_queries, f"ttj_queries {ttj_queries} doesn't equal to hj_queries {hj_queries}")
    ttj_target_queries, ttj_target_tuple_fetch, ttj_target_no_good_probe, ttj_target_no_good_construct, ttj_target_pass_context, \
        ttj_target_join_time, ttj_target_hash_table_build, ttj_target_predicate_evaluation, ttj_target_other = \
        [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
    hj_target_queries, hj_target_tuple_fetch, hj_target_no_good_probe, hj_target_no_good_construct, hj_target_pass_context, \
        hj_target_join_time, hj_target_hash_table_build, hj_target_predicate_evaluation, hj_target_other = \
        [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
    for i, query in enumerate(ttj_queries):
        if filter_queries(query, ordering):
            ttj_target_queries.append(query)
            hj_target_queries.append(query)
            ttj_target_tuple_fetch = np.append(ttj_target_tuple_fetch, ttj_tuple_fetch[i])
            hj_target_tuple_fetch = np.append(hj_target_tuple_fetch, hj_tuple_fetch[i])
            ttj_target_no_good_probe = np.append(ttj_target_no_good_probe, ttj_no_good_probe[i])
            hj_target_no_good_probe = np.append(hj_target_no_good_probe, hj_no_good_probe[i])
            ttj_target_no_good_construct = np.append(ttj_target_no_good_construct, ttj_no_good_construct[i])
            hj_target_no_good_construct = np.append(hj_target_no_good_construct, hj_no_good_construct[i])
            ttj_target_pass_context = np.append(ttj_target_pass_context, ttj_pass_context[i])
            hj_target_pass_context = np.append(hj_target_pass_context, hj_pass_context[i])
            ttj_target_join_time = np.append(ttj_target_join_time, ttj_join_time[i])
            hj_target_join_time = np.append(hj_target_join_time, hj_join_time[i])
            ttj_target_hash_table_build = np.append(ttj_target_hash_table_build, ttj_hash_table_build[i])
            hj_target_hash_table_build = np.append(hj_target_hash_table_build, hj_hash_table_build[i])
            ttj_target_predicate_evaluation = np.append(ttj_target_predicate_evaluation, ttj_predicate_evaluation[i])
            hj_target_predicate_evaluation = np.append(hj_target_predicate_evaluation, hj_predicate_evaluation[i])
            ttj_target_other = np.append(ttj_target_other, ttj_other[i])
            hj_target_other = np.append(hj_target_other, hj_other[i])

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
    plt.bar(br1, ttj_target_other, color='tab:blue', width=barWidth, label='Other')
    plt.bar(br1, ttj_target_predicate_evaluation, color='tab:olive', bottom=ttj_target_other, width=barWidth, label='Predicate Evaluation')
    plt.bar(br1, ttj_target_tuple_fetch, color='tab:orange', bottom=ttj_target_other + ttj_target_predicate_evaluation, width=barWidth,
            label='Tuple Fetch')
    plt.bar(br1, ttj_target_no_good_probe, color='tab:green', bottom=ttj_target_other + ttj_target_predicate_evaluation + ttj_target_tuple_fetch,
            width=barWidth, label='No-Good Probe')
    plt.bar(br1, ttj_target_no_good_construct, color='tab:pink', bottom=ttj_target_other + ttj_target_predicate_evaluation + ttj_target_tuple_fetch + ttj_target_no_good_probe,
            width=barWidth, label='No-Good Construct')
    plt.bar(br1, ttj_target_pass_context, color='tab:red',
            bottom=ttj_target_other + ttj_target_predicate_evaluation + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_no_good_construct, width=barWidth,
            label='PassContext')
    plt.bar(br1, ttj_target_join_time, color='tab:purple',
            bottom=ttj_target_other + ttj_target_predicate_evaluation + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_no_good_construct + ttj_target_pass_context,
            width=barWidth, label='Join')
    plt.bar(br1, ttj_target_hash_table_build, color='tab:brown',
            bottom=ttj_target_other + ttj_target_predicate_evaluation + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_no_good_construct + ttj_target_pass_context + ttj_target_join_time,
            width=barWidth, label='Hash Table Build')
    for x, y in zip(br1,
                    ttj_target_hash_table_build + ttj_target_other + ttj_target_predicate_evaluation + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_no_good_construct + ttj_target_pass_context + ttj_target_join_time):
        plt.text(x - barWidth / 2.0, y, 'TTJ')

    plt.bar(br2, hj_target_other, color='tab:blue', width=barWidth)
    plt.bar(br2, hj_target_predicate_evaluation, color='tab:olive', bottom=hj_target_other, width=barWidth)
    plt.bar(br2, hj_target_tuple_fetch, color='tab:orange', bottom=hj_target_other + hj_target_predicate_evaluation, width=barWidth)
    plt.bar(br2, hj_target_no_good_probe, color='tab:green', bottom=hj_target_other + hj_target_predicate_evaluation + hj_target_tuple_fetch,
            width=barWidth)
    plt.bar(br2, hj_target_no_good_construct, color='tab:pink', bottom=hj_target_other + hj_target_predicate_evaluation + hj_target_tuple_fetch + hj_target_no_good_probe,
            width=barWidth)
    plt.bar(br2, hj_target_pass_context, color='tab:red',
            bottom=hj_target_other + hj_target_predicate_evaluation + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_no_good_construct, width=barWidth)
    plt.bar(br2, hj_target_join_time, color='tab:purple',
            bottom=hj_target_other + hj_target_predicate_evaluation + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_no_good_construct + hj_target_pass_context,
            width=barWidth)
    plt.bar(br2, hj_target_hash_table_build, color='tab:brown',
            bottom=hj_target_other + hj_target_predicate_evaluation + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_no_good_construct + hj_target_pass_context + hj_target_join_time,
            width=barWidth)
    for x, y in zip(br1,
                    hj_target_hash_table_build + hj_target_other + hj_target_predicate_evaluation + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_no_good_construct + hj_target_pass_context + hj_target_join_time):
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


# def draw3(algorithm):
#     """
#     Like draw() but combine Other and Join as a total join time. The idea comes from LIP paper but not quite the same.
#     """
#     queries, runtime, tuple_fetch, no_good_probe, pass_context, join_time, hash_table_build, other = read_data(
#         algorithm)
#
#     total_join_time = join_time + other
#
#     # truncate queries name
#     truncate_queries = []
#     for query in queries:
#         replaced = re.sub('JoinTreeOptOrdering', '', query)
#         truncate_queries.append(replaced)
#
#     # plot bars in stack manner
#     plt.bar(truncate_queries, tuple_fetch, color='tab:orange')
#     plt.bar(truncate_queries, no_good_probe, bottom=tuple_fetch, color='tab:green')
#     plt.bar(truncate_queries, pass_context, bottom=tuple_fetch + no_good_probe, color='tab:red')
#     plt.bar(truncate_queries, total_join_time, bottom=tuple_fetch + no_good_probe + pass_context, color='tab:purple')
#     plt.bar(truncate_queries, hash_table_build, bottom=tuple_fetch + no_good_probe + pass_context + total_join_time,
#             color='tab:brown')
#     plt.xlabel("Queries")
#     plt.ylabel("Runtime (ms)")
#     plt.legend(["Tuple Fetch", "No-Good Probe", "PassContext", "Total Join Time", "Hash Table Build"])
#     plt.title(f"Execution Time Breakdown ({algorithm})")
#     plt.xticks(rotation=45, ha='right')
#     plt.show()


# def draw4(ordering):
#     """
#     Like draw2() but combine Other and Join as a total join time. The idea comes from LIP paper but not quite the same.
#     """
#
#     def filter_queries(query, ordering):
#         if ordering == "draft":
#             if "Opt" in query:
#                 return False
#             return True
#         if ordering == "opt":
#             if "Opt" in query:
#                 return True
#             return False
#
#     ttj_queries, ttj_runtime, ttj_tuple_fetch, \
#         ttj_no_good_probe, ttj_pass_context, ttj_join_time, ttj_hash_table_build, ttj_other = read_data("TTJ")
#     hj_queries, hj_runtime, hj_tuple_fetch, \
#         hj_no_good_probe, hj_pass_context, hj_join_time, hj_hash_table_build, hj_other = read_data("HJ")
#     check_argument(ttj_queries == hj_queries, f"ttj_queries {ttj_queries} doesn't equal to hj_queries {hj_queries}")
#     ttj_target_queries, ttj_target_tuple_fetch, ttj_target_no_good_probe, ttj_target_pass_context, \
#         ttj_target_join_time, ttj_target_hash_table_build, ttj_target_other = \
#         [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
#     hj_target_queries, hj_target_tuple_fetch, hj_target_no_good_probe, hj_target_pass_context, \
#         hj_target_join_time, hj_target_hash_table_build, hj_target_other = \
#         [], np.array([]), np.array([]), np.array([]), np.array([]), np.array([]), np.array([])
#     for i, query in enumerate(ttj_queries):
#         if filter_queries(query, ordering):
#             ttj_target_queries.append(query)
#             hj_target_queries.append(query)
#             ttj_target_tuple_fetch = np.append(ttj_target_tuple_fetch, ttj_tuple_fetch[i])
#             hj_target_tuple_fetch = np.append(hj_target_tuple_fetch, hj_tuple_fetch[i])
#             ttj_target_no_good_probe = np.append(ttj_target_no_good_probe, ttj_no_good_probe[i])
#             hj_target_no_good_probe = np.append(hj_target_no_good_probe, hj_no_good_probe[i])
#             ttj_target_pass_context = np.append(ttj_target_pass_context, ttj_pass_context[i])
#             hj_target_pass_context = np.append(hj_target_pass_context, hj_pass_context[i])
#             ttj_target_join_time = np.append(ttj_target_join_time, ttj_join_time[i])
#             hj_target_join_time = np.append(hj_target_join_time, hj_join_time[i])
#             ttj_target_hash_table_build = np.append(ttj_target_hash_table_build, ttj_hash_table_build[i])
#             hj_target_hash_table_build = np.append(hj_target_hash_table_build, hj_hash_table_build[i])
#             ttj_target_other = np.append(ttj_target_other, ttj_other[i])
#             hj_target_other = np.append(hj_target_other, hj_other[i])
#
#     hj_target_total_join_time = hj_target_join_time + hj_target_other
#     ttj_target_total_join_time = ttj_target_join_time + ttj_target_other
#
#
#     if ordering == "opt":
#         # truncate queries name
#         truncate_queries = []
#         for query in ttj_target_queries:
#             replaced = re.sub('JoinTreeOptOrdering', '', query)
#             truncate_queries.append(replaced)
#     else:
#         truncate_queries = ttj_target_queries
#
#     # set width of bar
#     barWidth = 0.25
#     fig = plt.subplots(figsize=(12, 8))
#
#     # Set position of bar on X axis
#     br1 = np.arange(len(truncate_queries))
#     br2 = [x + barWidth for x in br1]
#
#     # Make the plot
#     plt.bar(br1, ttj_target_tuple_fetch, color='tab:orange', width=barWidth,
#             label='Tuple Fetch')
#     plt.bar(br1, ttj_target_no_good_probe, color='tab:green', bottom=ttj_target_tuple_fetch,
#             width=barWidth, label='No-Good Probe')
#     plt.bar(br1, ttj_target_pass_context, color='tab:red',
#             bottom=ttj_target_tuple_fetch + ttj_target_no_good_probe, width=barWidth,
#             label='PassContext')
#     plt.bar(br1, ttj_target_total_join_time, color='tab:purple',
#             bottom=ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_pass_context,
#             width=barWidth, label='Join')
#     plt.bar(br1, ttj_target_hash_table_build, color='tab:brown',
#             bottom=ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_pass_context + ttj_target_total_join_time,
#             width=barWidth, label='Hash Table Build')
#     for x, y in zip(br1,
#                     ttj_target_hash_table_build + ttj_target_tuple_fetch + ttj_target_no_good_probe + ttj_target_pass_context + ttj_target_total_join_time):
#         plt.text(x - barWidth / 2.0, y, 'TTJ')
#
#     plt.bar(br2, hj_target_tuple_fetch, color='tab:orange', width=barWidth)
#     plt.bar(br2, hj_target_no_good_probe, color='tab:green', bottom=hj_target_tuple_fetch,
#             width=barWidth)
#     plt.bar(br2, hj_target_pass_context, color='tab:red',
#             bottom=hj_target_tuple_fetch + hj_target_no_good_probe, width=barWidth)
#     plt.bar(br2, hj_target_total_join_time, color='tab:purple',
#             bottom=hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_pass_context,
#             width=barWidth)
#     plt.bar(br2, hj_target_hash_table_build, color='tab:brown',
#             bottom=hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_pass_context + hj_target_total_join_time,
#             width=barWidth)
#     for x, y in zip(br1,
#                     hj_target_hash_table_build + hj_target_tuple_fetch + hj_target_no_good_probe + hj_target_pass_context + hj_target_total_join_time):
#         plt.text(x + barWidth / 2.0, y, 'HJ')
#
#     # Adding Xticks
#     plt.xlabel('Queries', fontweight='bold', fontsize=15)
#     plt.ylabel('Runtime (ms)', fontweight='bold', fontsize=15)
#     plt.xticks([r + barWidth for r in range(len(truncate_queries))],
#                truncate_queries)
#     if ordering == 'draft':
#         ordering_name = 'Draft Ordering'
#     else:
#         ordering_name = 'Opt Ordering'
#     plt.title(f"Execution Time Breakdown Comparison ({ordering_name})")
#     plt.legend()
#     plt.show()


if __name__ == "__main__":
    # draw("TTJ")
    # draw("HJ")
    # draw2("draft")
    draw2("opt")
    # draw3("TTJ")
    # draw3("HJ")
    # draw4("draft")
    # draw4("opt")