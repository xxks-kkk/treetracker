"""
Obtain intermediate results and result set size from the query
as a way to understand the performance gap between TTJ and YA+.

This is what Remy suggested in 07/17/24 meeting.
"""
import csv
import json
import os
import re
from pathlib import Path

from matplotlib import pyplot as plt

from plot.constants import DATA_SOURCE_CSV, ALGORITHMS_TO_PLOT, HJ, TTJ, Yannakakis1Pass, COLUMN_RIGHT_BOUND
from plot.cost_model4 import extract_data_from_csv
from plot.utility import check_argument

PROJECT_DIR = Path.home() / "projects" / "treetracker2"

JOB_AGG_STATS_DIR = PROJECT_DIR / "results" / "others" / "simple-cost-model-with-predicates" / "hj_ordering_hj"


def get_job_full_path(csv_name):
    return Path.home() / "projects" / "treetracker2" / "results" / "job" / "with_predicates" / "hj_ordering_hj" / csv_name


class CSVWriter():
    filename = None
    fp = None
    writer = None

    def __init__(self, filename):
        self.filename = filename
        self.fp = open(self.filename, 'w', encoding='utf8')
        self.writer = csv.writer(self.fp, delimiter=',', lineterminator='\n')

    def close(self):
        self.fp.close()

    def write(self, *args):
        self.writer.writerow(args)

    def size(self):
        return os.path.getsize(self.filename)

    def fname(self):
        return self.filename

def natural_sort_key(s, _nsre=re.compile('([0-9]+)')):
    return [int(text) if text.isdigit() else text.lower()
            for text in _nsre.split(s)]


def process_job(target_queries):
    """
    We first gather intermediate result size produced by each join and final output size for TTJ.
    The output is a csv file with the format

    --
    query,result
    5a, [movie_companies:3, title:2, company_type:1, movie_info:1, it:0]
    ...
    --

    To understand the meaning of the file, consider 5a above. The order of relations appeared represents
    the join order. In this case, the join order is [movie_companies, title, company_type, movie_info],
    which represents the order of relations in a left-deep plan from left to right.

    The last number, e.g., 0, represents the query output size (count(*) without final selection predicate
    appeared in the benchmark queries). The rest number represents the intermediate result size.

           0  |
             join8
         1  /    \
           join6 it
        1  /   \
         join4 mi
      2  /   \
       join2 ct
     3 /  \
     mc  title

    The first 3 is the size of output of mc, which is also the size of input to join2.
    The second 2 is |movie_companies \join title|, which is the size of input to join4.
    The third 1 is |movie_companies \join title \join company_type|, which is the size of input to join6
    The fourth 1 is |movie_companies \join title \join company_type \join movie_info|, which is the size of input to join8.
    The fifith 0 is the result output size.
    """
    imdb_relations = ['aka_name',
                      "aka_title",
                      "cast_info",
                      "char_name",
                      "comp_cast_type",
                      "company_name",
                      "company_type",
                      "complete_cast",
                      "info_type",
                      "keyword",
                      "kind_type",
                      "link_type",
                      "movie_companies",
                      "movie_info_idx",
                      "movie_keyword",
                      "movie_link",
                      "name",
                      "role_type",
                      "title",
                      "movie_info",
                      "person_info"]

    def generate_data(json_data):
        def obtain_query_name(raw):
            parts = raw.split('.')
            beginning_of_suffix = parts[-1].index('OptJoinTreeOptOrderingShallowHJOrdering')
            ending_of_prefix = parts[-1].index('Query') + len('Query')
            return parts[-1][ending_of_prefix:beginning_of_suffix]

        def process_join(json_data, res, intermediate_size):
            for key, val in json_data.items():
                if 'join' in key and 'joinTime' not in key:
                    intermediate_result_size = val['numberOfR1Assignments']
                    relations = []
                    for key2, val2 in val.items():
                        for imdb_relation in imdb_relations:
                            if imdb_relation in key2:
                                relations.append(key2)
                                break
                    if len(relations) == 1:
                        res.append((relations[0], intermediate_size))
                    elif len(relations) == 2:
                        # append order matters
                        res.append((relations[1], intermediate_size))
                        res.append((relations[0], intermediate_result_size))
                    process_join(json_data[key], res, intermediate_result_size)

        aggregation_stats = json_data['Aggregation Stats']
        query = obtain_query_name(aggregation_stats['queryName'])
        result_set_size = aggregation_stats['resutSetSize']
        res = []
        process_join(json_data, res, result_set_size)
        res.reverse()
        return query, res

    mycsv = CSVWriter('intermediate_result.csv')
    result_list = []
    for stats_file in list(Path(JOB_AGG_STATS_DIR).glob('TTJHP*.json')):
        print(f"processing {stats_file} ...")
        with open(stats_file.as_posix(), "r") as fd:
            json_data = json.load(fd)
            query, result = generate_data(json_data)
            if len(target_queries) == 0 or query in target_queries:
                result_list.append((query, result))
    result_list.sort(key=lambda x: natural_sort_key(x[0]))
    for query, result in result_list:
            mycsv.write(query, result)
    mycsv.close()
    return result_list


def plot_job():
    """
    We draw the intermediate results produced by each join of each query in JOB to see if
    we can observe any pattern.
    """
    def perform_grouping(raw_data: dict) -> dict:
        """
        We aggregate all the queries with the same flight, e.g., 1a, 1b, 1c together
        """
        grouped_data = dict()
        for algorithm in raw_data.keys():
            grouped_data[algorithm] = []
            data = raw_data[algorithm]
            grouped_data[algorithm].append(data['query1a'])
            grouped_data[algorithm].append(data['query1b'])
            grouped_data[algorithm].append(data['query1c'])
            grouped_data[algorithm].append(data['query1d'])
            grouped_data[algorithm].append(data['query2a'])
            grouped_data[algorithm].append(data['query2b'])
            grouped_data[algorithm].append(data['query2c'])
            grouped_data[algorithm].append(data['query2d'])
            grouped_data[algorithm].append(data['query3a'])
            grouped_data[algorithm].append(data['query3b'])
            grouped_data[algorithm].append(data['query3c'])
            grouped_data[algorithm].append(data['query4a'])
            grouped_data[algorithm].append(data['query4b'])
            grouped_data[algorithm].append(data['query4c'])
            grouped_data[algorithm].append(data['query5a'])
            grouped_data[algorithm].append(data['query5b'])
            grouped_data[algorithm].append(data['query5c'])
            grouped_data[algorithm].append(data['query6a'])
            grouped_data[algorithm].append(data['query6b'])
            grouped_data[algorithm].append(data['query6c'])
            grouped_data[algorithm].append(data['query6d'])
            grouped_data[algorithm].append(data['query6e'])
            grouped_data[algorithm].append(data['query6f'])
            grouped_data[algorithm].append(data['query7a'])
            grouped_data[algorithm].append(data['query7b'])
            grouped_data[algorithm].append(data['query7c'])
            grouped_data[algorithm].append(data['query8a'])
            grouped_data[algorithm].append(data['query8b'])
            grouped_data[algorithm].append(data['query8c'])
            grouped_data[algorithm].append(data['query8d'])
            grouped_data[algorithm].append(data['query9a'])
            grouped_data[algorithm].append(data['query9b'])
            grouped_data[algorithm].append(data['query9c'])
            grouped_data[algorithm].append(data['query9d'])
            grouped_data[algorithm].append(data['query10a'])
            grouped_data[algorithm].append(data['query10b'])
            grouped_data[algorithm].append(data['query10c'])
            grouped_data[algorithm].append(data['query11a'])
            grouped_data[algorithm].append(data['query11b'])
            grouped_data[algorithm].append(data['query11c'])
            grouped_data[algorithm].append(data['query11d'])
            grouped_data[algorithm].append(data['query12a'])
            grouped_data[algorithm].append(data['query12b'])
            grouped_data[algorithm].append(data['query12c'])
            grouped_data[algorithm].append(data['query13a'])
            grouped_data[algorithm].append(data['query13b'])
            grouped_data[algorithm].append(data['query13c'])
            grouped_data[algorithm].append(data['query13d'])
            grouped_data[algorithm].append(data['query14a'])
            grouped_data[algorithm].append(data['query14b'])
            grouped_data[algorithm].append(data['query14c'])
            grouped_data[algorithm].append(data['query15a'])
            grouped_data[algorithm].append(data['query15b'])
            grouped_data[algorithm].append(data['query15c'])
            grouped_data[algorithm].append(data['query15d'])
            grouped_data[algorithm].append(data['query16a'])
            grouped_data[algorithm].append(data['query16b'])
            grouped_data[algorithm].append(data['query16c'])
            grouped_data[algorithm].append(data['query16d'])
            grouped_data[algorithm].append(data['query17a'])
            grouped_data[algorithm].append(data['query17b'])
            grouped_data[algorithm].append(data['query17c'])
            grouped_data[algorithm].append(data['query17d'])
            grouped_data[algorithm].append(data['query17e'])
            grouped_data[algorithm].append(data['query17f'])
            grouped_data[algorithm].append(data['query18a'])
            grouped_data[algorithm].append(data['query18b'])
            grouped_data[algorithm].append(data['query18c'])
            grouped_data[algorithm].append(data['query19a'])
            grouped_data[algorithm].append(data['query19b'])
            grouped_data[algorithm].append(data['query19c'])
            grouped_data[algorithm].append(data['query19d'])
            grouped_data[algorithm].append(data['query20a'])
            grouped_data[algorithm].append(data['query20b'])
            grouped_data[algorithm].append(data['query20c'])
            grouped_data[algorithm].append(data['query21a'])
            grouped_data[algorithm].append(data['query21b'])
            grouped_data[algorithm].append(data['query21c'])
            grouped_data[algorithm].append(data['query22a'])
            grouped_data[algorithm].append(data['query22b'])
            grouped_data[algorithm].append(data['query22c'])
            grouped_data[algorithm].append(data['query22d'])
            grouped_data[algorithm].append(data['query23a'])
            grouped_data[algorithm].append(data['query23b'])
            grouped_data[algorithm].append(data['query23c'])
            grouped_data[algorithm].append(data['query24a'])
            grouped_data[algorithm].append(data['query24b'])
            grouped_data[algorithm].append(data['query25a'])
            grouped_data[algorithm].append(data['query25b'])
            grouped_data[algorithm].append(data['query25c'])
            grouped_data[algorithm].append(data['query26a'])
            grouped_data[algorithm].append(data['query26b'])
            grouped_data[algorithm].append(data['query26c'])
            grouped_data[algorithm].append(data['query27a'])
            grouped_data[algorithm].append(data['query27b'])
            grouped_data[algorithm].append(data['query27c'])
            grouped_data[algorithm].append(data['query28a'])
            grouped_data[algorithm].append(data['query28b'])
            grouped_data[algorithm].append(data['query28c'])
            grouped_data[algorithm].append(data['query29a'])
            grouped_data[algorithm].append(data['query29b'])
            grouped_data[algorithm].append(data['query29c'])
            grouped_data[algorithm].append(data['query30a'])
            grouped_data[algorithm].append(data['query30b'])
            grouped_data[algorithm].append(data['query30c'])
            grouped_data[algorithm].append(data['query31a'])
            grouped_data[algorithm].append(data['query31b'])
            grouped_data[algorithm].append(data['query31c'])
            grouped_data[algorithm].append(data['query32a'])
            grouped_data[algorithm].append(data['query32b'])
            grouped_data[algorithm].append(data['query33a'])
            grouped_data[algorithm].append(data['query33b'])
            grouped_data[algorithm].append(data['query33c'])
        return grouped_data

    job_plot = {
        DATA_SOURCE_CSV: "benchmarkjobwithpredicatesfixedhjorderingshallow-result-2024-07-07t17:19:27.127911benchmarkjobwithpredicatesfixedhjorderingshallow-result-2024-07-22t16:51:01.516062_perf_report.csv",
        ALGORITHMS_TO_PLOT: [HJ, TTJ, Yannakakis1Pass],
        COLUMN_RIGHT_BOUND: 114
    }
    prem_data = extract_data_from_csv(get_job_full_path(job_plot[DATA_SOURCE_CSV]),
                                      column_range=[1, job_plot[COLUMN_RIGHT_BOUND]])

    data = dict()
    for algorithm in prem_data.keys():
        if algorithm in job_plot[ALGORITHMS_TO_PLOT]:
            data[algorithm] = prem_data[algorithm]

    for dps in data.values():
        check_argument(len(dps) == job_plot[COLUMN_RIGHT_BOUND] - 1,
                       f"some query data is missing. There should be 113 dps. Instead, we have {len(dps)}")

    labels = ["1a", "1b", "1c", "1d",
              "2a", "2b", "2c", "2d",
              "3a", "3b", "3c",
              "4a", "4b", "4c",
              "5a", "5b", "5c",
              "6a", "6b", "6c", "6d", "6e", "6f",
              "7a", "7b", "7c",
              "8a", "8b", "8c", "8d",
              "9a", "9b", "9c", "9d",
              "10a", "10b", "10c",
              "11a", "11b", "11c", "11d",
              "12a", "12b", "12c",
              "13a", "13b", "13c", "13d",
              "14a", "14b", "14c",
              "15a", "15b", "15c", "15d",
              "16a", "16b", "16c", "16d",
              "17a", "17b", "17c", "17d", "17e", "17f",
              "18a", "18b", "18c",
              "19a", "19b", "19c", "19d",
              "20a", "20b", "20c",
              "21a", "21b", "21c",
              "22a", "22b", "22c", "22d",
              "23a", "23b", "23c",
              "24a", "24b",
              "25a", "25b", "25c",
              "26a", "26b", "26c",
              "27a", "27b", "27c",
              "28a", "28b", "28c",
              "29a", "29b", "29c",
              "30a", "30b", "30c",
              "31a", "31b", "31c",
              "32a", "32b",
              "33a", "33b", "33c"]
    grouped_data = perform_grouping(data)

    data_speedup = dict()
    for algorithm, dps in grouped_data.items():
        ya = grouped_data[Yannakakis1Pass]
        data_speedup[algorithm] = \
            [round(ya_time / algorithm_time, 1) for ya_time, algorithm_time in zip(ya, grouped_data[algorithm])]
    del data_speedup[HJ]
    del data_speedup[Yannakakis1Pass]

    equal_queries = []
    better_queries = []
    worse_queries = []

    for idx, speed_up in enumerate(data_speedup[TTJ]):
        if speed_up >= 0.9 and speed_up <= 1.1:
            equal_queries.append(labels[idx])
        elif speed_up >= 1.2:
            better_queries.append(labels[idx])
        else:
            worse_queries.append(labels[idx])

    def obtain_color(query):
        if query in equal_queries:
            return "y"
        elif query in better_queries:
            return "g"
        else:
            return "r"

    result_list = process_job([])
    for dps in result_list:
        y = []
        for relation, intermediate_result_size in dps[1]:
            y.append(intermediate_result_size)
        x = list(range(len(y)))
        color = obtain_color(dps[0])
        if color == 'y' or color == 'r' or color == 'g':
            plt.plot(x, y, color, label=dps[0])
    plt.xlabel("joined relations")
    plt.ylabel("intermediate result size")
    # plt.legend()
    plt.show()


if __name__ == "__main__":
    # process_job([])
    # process_job(["6a", "6b", "6c", "6d", "6e", "7b", "12b", "17a", "32a"])
    plot_job()