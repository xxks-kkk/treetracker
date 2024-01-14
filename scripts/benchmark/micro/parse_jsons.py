"""
Parse json statistics and create csvs for future analysis
"""
import csv
import json
from pathlib import Path


def generate_csv_from_jsons(list_of_jsons, algorithm, directory):
    def get_query_name(json_file):
        if 'Query1aOptJoinTreeOptOrdering' in json_file:
            return 'Q1aOptJoinTreeOptOrdering'
        if 'Query1bOptJoinTreeOptOrdering' in json_file:
            return 'Q1bOptJoinTreeOptOrdering'
        if 'Query2aOptJoinTreeOptOrdering' in json_file:
            return 'Q2aOptJoinTreeOptOrdering'
        if 'Query2bOptJoinTreeOptOrdering' in json_file:
            return 'Q2bOptJoinTreeOptOrdering'
        if 'Query2cOptJoinTreeOptOrdering' in json_file:
            return 'Q2cOptJoinTreeOptOrdering'
        if 'Query3bOptJoinTreeOptOrdering' in json_file:
            return 'Q3bOptJoinTreeOptOrdering'
        if 'Query4aOptJoinTreeOptOrdering' in json_file:
            return 'Q4aOptJoinTreeOptOrdering'
        if 'Query7aOptJoinTreeOptOrdering' in json_file:
            return 'Q7aOptJoinTreeOptOrdering'
        if 'Query8cOptJoinTreeOptOrdering' in json_file:
            return 'Q8cOptJoinTreeOptOrdering'
        if 'Query10aOptJoinTreeOptOrdering' in json_file:
            return 'Q10aOptJoinTreeOptOrdering'
        if 'Query11cOptJoinTreeOptOrdering' in json_file:
            return 'Q11cOptJoinTreeOptOrdering'
        if 'Query11dOptJoinTreeOptOrdering' in json_file:
            return 'Q11dOptJoinTreeOptOrdering'
        if 'Query12bOptJoinTreeOptOrdering' in json_file:
            return 'Q12bOptJoinTreeOptOrdering'
        if 'Query12cOptJoinTreeOptOrdering' in json_file:
            return 'Q12cOptJoinTreeOptOrdering'
        if 'Query5aOptJoinTreeOptOrdering' in json_file:
            return 'Q5aOptJoinTreeOptOrdering'
        if 'Query5bOptJoinTreeOptOrdering' in json_file:
            return 'Q5bOptJoinTreeOptOrdering'
        if 'Query5cOptJoinTreeOptOrdering' in json_file:
            return 'Q5cOptJoinTreeOptOrdering'
        if 'Query6fOptJoinTreeOptOrdering' in json_file:
            return 'Q6fOptJoinTreeOptOrdering'
        if 'Query8aOptJoinTreeOptOrdering' in json_file:
            return 'Q8aOptJoinTreeOptOrdering'
        if 'Query9dOptJoinTreeOptOrdering' in json_file:
            return 'Q9dOptJoinTreeOptOrdering'
        if 'Query15cOptJoinTreeOptOrdering' in json_file:
            return 'Q15cOptJoinTreeOptOrdering'
        if 'Query25aOptJoinTreeOptOrdering' in json_file:
            return 'Q25aOptJoinTreeOptOrdering'
        if 'Query25cOptJoinTreeOptOrdering' in json_file:
            return 'Q25cOptJoinTreeOptOrdering'
        if 'Query31aOptJoinTreeOptOrdering' in json_file:
            return 'Q31aOptJoinTreeOptOrdering'
        if 'Query31cOptJoinTreeOptOrdering' in json_file:
            return 'Q31cOptJoinTreeOptOrdering'
        if 'Query33bOptJoinTreeOptOrdering' in json_file:
            return 'Q33bOptJoinTreeOptOrdering'

    with open(f'{algorithm}.csv', "w") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Query", "Runtime", "Total Tuple Fetching Time",
                         "No-good List Probing Time",
                         "No-good List Construct Time",
                         "PassContextWorkTime", "JoinTime", "HashTableBuildTime",
                         "Predicate Evaluation",
                         "Other"])
        for json_file in list_of_jsons:
            row = []
            row.append(get_query_name(json_file))
            json_file_path = Path(directory).joinpath(json_file)
            with open(json_file_path) as f:
                data = json.load(f)
                agg_stats = data["Aggregation Stats"]
                row.append(agg_stats['runtime (ms)'])
                row.append(agg_stats['tupleFetchingTime for all table scan operators (ms)'])
                rk_fetching_time = float(agg_stats['R_k tupleFetchingTime (ms)'])
                if algorithm == "ttj":
                    row.append(agg_stats["noGoodListProbingTime (ms)"])
                    row.append(agg_stats["noGoodListConstructTime (ms)"])
                    row.append(agg_stats["passContextWorkTime (ms)"])
                else:
                    row.append(0)
                    row.append(0)
                    row.append(0)
                row.append(agg_stats["totalJoinTime (ms)"])
                row.append(float(agg_stats["hashTableBuildTime (ms)"]) -
                           float(agg_stats['tupleFetchingTime for all table scan operators (ms)']) +
                           rk_fetching_time)
                row.append(agg_stats["predicateEvaluationTime (ms)"])
                if algorithm == "ttj":
                    row.append(float(agg_stats['runtime (ms)']) -
                               float(agg_stats['tupleFetchingTime for all table scan operators (ms)']) -
                               float(agg_stats["noGoodListProbingTime (ms)"]) -
                               float(agg_stats["passContextWorkTime (ms)"]) -
                               float(agg_stats["noGoodListConstructTime (ms)"]) -
                               float(agg_stats["totalJoinTime (ms)"]) -
                               (float(agg_stats["hashTableBuildTime (ms)"]) -
                                float(agg_stats['tupleFetchingTime for all table scan operators (ms)']) +
                                rk_fetching_time) -
                               float(agg_stats["predicateEvaluationTime (ms)"]))
                else:
                    row.append(float(agg_stats['runtime (ms)']) -
                               float(agg_stats['tupleFetchingTime for all table scan operators (ms)']) -
                               float(agg_stats["totalJoinTime (ms)"]) -
                               (float(agg_stats["hashTableBuildTime (ms)"]) -
                                float(agg_stats['tupleFetchingTime for all table scan operators (ms)']) +
                                rk_fetching_time) -
                               float(agg_stats["predicateEvaluationTime (ms)"]))
                writer.writerow(row)


if __name__ == "__main__":
    hash_join_list_of_jsons = ["HASH_JOIN_org.zhu45.treetracker.benchmark.job.q4.Query4aOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q2.Query2cOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q1.Query1bOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q33.Query33bOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q15.Query15cOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q3.Query3bOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q10.Query10aOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q31.Query31aOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q31.Query31cOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q25.Query25aOptJoinTreeOptOrdering.json",
                               "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q25.Query25cOptJoinTreeOptOrdering.json"]
    ttj_list_of_jsons = ["TTJHP_org.zhu45.treetracker.benchmark.job.q4.Query4aOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q2.Query2cOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q1.Query1bOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q33.Query33bOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q15.Query15cOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q3.Query3bOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q10.Query10aOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q31.Query31aOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q31.Query31cOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q25.Query25aOptJoinTreeOptOrdering.json",
                         "TTJHP_org.zhu45.treetracker.benchmark.job.q25.Query25cOptJoinTreeOptOrdering.json"]
    generate_csv_from_jsons(hash_join_list_of_jsons,
                            "hash_join",
                            "/home/zeyuanhu/projects/challenge-set-gitlab/results/others/simple-cost-model-with-predicates/")
    generate_csv_from_jsons(ttj_list_of_jsons,
                            "ttj",
                            "/home/zeyuanhu/projects/challenge-set-gitlab/results/others/simple-cost-model-with-predicates/")
