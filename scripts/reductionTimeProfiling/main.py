"""
Main to drive exp 1.3 Runtime breakdown

The steps are:
1. Generate java file based on `ReductionTimeProfiling.javat` for profiling specific algorithm on specific benchmark
2. Execute `mvn clean install -DskipTests -P prod` to build the source
3. Perform profiling on the engine and generate profile result
4. Parse the profile result and produce statistics related to reduction time percentage
"""
import csv
import subprocess

from plot.utility import check_argument
from reductionTimeProfiling.profile import create_profile_script, execute_profile_script, construct_profile_result, \
    get_profile_result_dir
from reductionTimeProfiling.proggen import generate_java, Benchmark, Algorithm, SSBQuery, get_mvn_root_dir, \
    list_all_public_fields, TTJTPCH, YannakakisTPCH, YannakakisBTPCH, TTJHPJOB, Yannakakis1PassJOB, HASHJOINJOB


def mvn_bld():
    return_code = subprocess.call("mvn clean install -DskipTests -P prod", shell=True, cwd=get_mvn_root_dir())
    return return_code

def run(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery, regenerate_profile: bool):
    if regenerate_profile:
        generate_java(benchmark, algorithm, query)
        return_code = mvn_bld()
        check_argument(return_code == 0, f"mvn bld fail: {return_code}")
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

def profile_ssb():
    ssb_queries = list_all_public_fields(SSBQuery)
    generate_aggregate_statistics_csv(Benchmark.ssb, Algorithm.TTJ,
                                      ssb_queries,
                                      False)
    generate_aggregate_statistics_csv(Benchmark.ssb, Algorithm.LIP,
                                      ssb_queries,
                                      False)
    generate_aggregate_statistics_csv(Benchmark.ssb, Algorithm.Yannakakis,
                                      ssb_queries,
                                      False)
    generate_aggregate_statistics_csv(Benchmark.ssb, Algorithm.YannakakisB,
                                      ssb_queries,
                                      False)

def profile_tpch():
    ttjhp_tpch_queries = list_all_public_fields(TTJTPCH)
    generate_aggregate_statistics_csv(Benchmark.tpch, Algorithm.TTJ,
                                      ttjhp_tpch_queries,
                                      True)
    yannakakis_tpch_queries = list_all_public_fields(YannakakisTPCH)
    generate_aggregate_statistics_csv(Benchmark.tpch, Algorithm.Yannakakis,
                                      yannakakis_tpch_queries,
                                      True)
    yannakakisB_tpch_queries = list_all_public_fields(YannakakisBTPCH)
    generate_aggregate_statistics_csv(Benchmark.tpch, Algorithm.YannakakisB,
                                      yannakakisB_tpch_queries,
                                      True)

def profile_job():
    # ttjhp_job_queries = list_all_public_fields(TTJHPJOB)
    # generate_aggregate_statistics_csv(Benchmark.job, Algorithm.TTJ,
    #                                   ttjhp_job_queries,
    #                                   True)
    yannakakis1Pass_job_queries = list_all_public_fields(Yannakakis1PassJOB)
    generate_aggregate_statistics_csv(Benchmark.job, Algorithm.Yannakakis1Pass,
                                      yannakakis1Pass_job_queries,
                                      True)
    # hj_job_queries = list_all_public_fields(HASHJOINJOB)
    # generate_aggregate_statistics_csv(Benchmark.job, Algorithm.HJ,
    #                                   hj_job_queries,
    #                                   True)

if __name__ == "__main__":
    # profile_tpch()
    # profile_ssb()
    profile_job()
