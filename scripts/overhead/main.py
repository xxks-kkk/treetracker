"""
Main to measure the overhead of deleting tuples in TTJ (for PODS 25')

The steps are:
1. Generate java file based on `ReductionTimeProfilingJOB.javat` for profiling specific algorithm on specific benchmark
2. Execute `mvn clean install -DskipTests -P prod` to build the source
3. Perform profiling on the engine and generate profile result
4. Parse the profile result and produce statistics related to reduction time percentage
"""
import csv
import subprocess

from profile import create_profile_script, execute_profile_script, construct_profile_result, \
    get_profile_result_dir
from proggen import generate_java_job, Benchmark, Algorithm, get_mvn_root_dir, \
    list_all_public_fields, TTJHPJOB


def mvn_bld():
    subprocess.run("mvn clean install -DskipTests -P prod", shell=True, cwd=get_mvn_root_dir(), check=True)

def run(benchmark: Benchmark, algorithm: Algorithm, query: TTJHPJOB, regenerate_profile: bool):
    if regenerate_profile:
        generate_java_job(benchmark, algorithm, query)
        mvn_bld()
        create_profile_script(benchmark, algorithm, query)
        execute_profile_script(benchmark, algorithm, query)
    return construct_profile_result(benchmark, algorithm, query)

def generate_aggregate_statistics_csv(benchmark: Benchmark, algorithm: Algorithm, queries, regenerate_profile: bool):
    profile_result_stats_agg = dict()
    fields = ['stats']
    stats_fields = []
    for i, query in enumerate(queries):
        fields.append(query)
        profile_result_stats_agg[query] = run(benchmark, algorithm, query, regenerate_profile)
        if i == 0:
            stats_fields = list(vars(profile_result_stats_agg[query]).keys())
    filename = get_profile_result_dir() / f"{benchmark}_{algorithm}_profiling_agg.csv"
    print(f"write to {filename}")
    with open(filename, 'w') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(fields)
        for stats_field in stats_fields:
            row = [stats_field]
            for query in fields[1:]:
                row.append(getattr(profile_result_stats_agg[query], stats_field))
            csvwriter.writerow(row)

def profile_job():
    ttjhp_job_queries = list_all_public_fields(TTJHPJOB)
    generate_aggregate_statistics_csv(Benchmark.job, Algorithm.TTJ,
                                      ttjhp_job_queries,
                                      False)

if __name__ == "__main__":
    profile_job()
