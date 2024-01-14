"""
Generate data for two-way join query A(s) \join B(s) with different parameters
"""
import csv
from pathlib import Path

import numpy as np
import psycopg2

from plot.utility import check_argument


def write_csv(header, rows, filename):
    with open(filename, "w") as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)
        csvwriter.writerows(rows)


def convert_columns_to_rows(columns):
    return np.array(columns).T.tolist()


def get_header_from_csv(filename):
    with open(filename) as csv_file:
        csv_reader = csv.DictReader(csv_file)
        dict_from_csv = dict(list(csv_reader)[0])
        list_of_column_names = list(dict_from_csv.keys())
        return list_of_column_names


def generate_relations(semijoin_selectivity: float, dup_ratio: float, matching_tuples: int, size_of_A: int, size_of_B: int):
    """
    Generate relations for 2-way join B(s) \join A(s)
    :param semijoin_selectivity: semijoin selectivity of A, |A \leftsemijoin B| / |A|
    :param dup_ratio: fraction of dangling tuples that can be filtered out by no-good list
    :param matching_tuples: # of join tuples from B for a given joinable tuple from A
    :param size_of_A: size of A
    :param size_of_B: size of B
    :return: csv file names for the generated relation A and B
    """
    rng = np.random.default_rng()

    # Generate relation A
    size_of_joinable_tuples_in_A = float(semijoin_selectivity * size_of_A)
    check_argument(size_of_joinable_tuples_in_A.is_integer(),
                   f"semijoin_selectivity {semijoin_selectivity} * size_of_A {size_of_A} is not integer")
    size_of_joinable_tuples_in_A = int(size_of_joinable_tuples_in_A)
    joinable_tuples_A = np.arange(0, size_of_joinable_tuples_in_A)
    dangling_tuples_A = np.arange(size_of_joinable_tuples_in_A, size_of_A)
    s_column_for_A = np.concatenate((joinable_tuples_A, dangling_tuples_A))
    np.random.shuffle(s_column_for_A)

    # Generate relation B
    size_of_joinable_tuples_in_B = size_of_joinable_tuples_in_A * float(matching_tuples)
    check_argument(size_of_B - size_of_joinable_tuples_in_B >= 0,
                   f"size_of_B {size_of_B} is smaller than size_of_joinable_tuples_in_B {size_of_joinable_tuples_in_B}")
    joinable_tuples_B = np.repeat(joinable_tuples_A, matching_tuples)
    size_of_dangling_tuples_in_B = size_of_B - size_of_joinable_tuples_in_B
    if size_of_dangling_tuples_in_B > 0:
        size_of_duplicate_dangling_tuples_in_B = size_of_dangling_tuples_in_B * float(dup_ratio)
        check_argument(size_of_duplicate_dangling_tuples_in_B.is_integer(),
                       f"size_of_duplicate_dangling_tuples_in_B {size_of_duplicate_dangling_tuples_in_B} is not integer")
        size_of_distinct_dangling_tuples_in_B = size_of_dangling_tuples_in_B - size_of_duplicate_dangling_tuples_in_B
        distinct_dangling_tuples_B = np.arange(size_of_A, size_of_B + size_of_A)
        random_tuple_from_distinct_dangling_tuples_B = distinct_dangling_tuples_B[rng.integers(0, size_of_distinct_dangling_tuples_in_B)]
        duplicate_dangling_tuples_B = np.repeat(random_tuple_from_distinct_dangling_tuples_B, size_of_duplicate_dangling_tuples_in_B)
    else:
        distinct_dangling_tuples_B = np.empty(0, int)
        duplicate_dangling_tuples_B = np.empty(0, int)
    s_column_for_B = np.concatenate((joinable_tuples_B, distinct_dangling_tuples_B, duplicate_dangling_tuples_B))
    np.random.shuffle(s_column_for_B)

    fields = ["s"]
    A_csv = f"ss_{str(semijoin_selectivity).replace('.', '')}_dr_{str(dup_ratio).replace('.', '')}_mt_{matching_tuples}_sa_{size_of_A}_sb_{size_of_B}_A.csv"
    B_csv = f"ss_{str(semijoin_selectivity).replace('.', '')}_dr_{str(dup_ratio).replace('.', '')}_mt_{matching_tuples}_sa_{size_of_A}_sb_{size_of_B}_B.csv"
    write_csv(fields, convert_columns_to_rows([s_column_for_A.tolist()]), A_csv)
    write_csv(fields, convert_columns_to_rows([s_column_for_B.tolist()]), B_csv)
    return Path(A_csv).absolute(), Path(B_csv).absolute()


def load_csv_into_postgres(*args):
    schema = "microbenchmark"
    for csv_file_name in args:
        table_name = Path(csv_file_name).stem
        with psycopg2.connect(database="postgres",
                              user='postgres', password='password',
                              host='127.0.0.1', port='5432') as conn:
            cursor = conn.cursor()
            sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
            cursor.execute(sql)
            column_names = get_header_from_csv(csv_file_name)
            relation = f"{table_name}({(lambda x: '.'.join(x))([x + ' int' for x in column_names])})"
            sql = f"CREATE TABLE IF NOT EXISTS {schema}.{relation};"
            cursor.execute(sql)
            sql2 = f"COPY {schema}.{table_name} FROM '{csv_file_name}' DELIMITER ',' CSV HEADER;"
            print(sql2)
            cursor.execute(sql2)


if __name__ == "__main__":
    A_csv, B_csv = generate_relations(semijoin_selectivity=1, dup_ratio=0.5, matching_tuples=40000, size_of_A=1, size_of_B=40000)
    load_csv_into_postgres(A_csv, B_csv)
