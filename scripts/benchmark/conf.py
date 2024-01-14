"""
Configuration file
"""
from enum import Enum

JAVA_EXECUTION_COMMANDS_FILE="JAVA_EXECUTION_COMMANDS_FILE"
SUPPORT_BENCHMARK_QUERIES="SUPPORT_BENCHMARK_QUERIES"
BENCHMARK_TO_RUN="BENCHMARK_TO_RUN"
LOG_DIR="LOG_DIR"
BENCHMARK_NAME="BENCHMARK_NAME"
ALGO_TO_RUN="ALGO_TO_RUN"

SRC_DIR=r"/home/zeyuanhu/projects/challenge-set-gitlab/treeTracker"
MVN_BLD_CMD='/usr/bin/mvn clean install -DskipTests'

JOB = "JOB"
FIND_WORST_CASE_JOIN_ORDERING = "FindWorstCaseJoinOrdering"
BENCHMARK_PATTERNS = "BenchmarkPatterns"
GATHER_ADDITIONAL_STATS = "GatherAdditionalStats"

JOB_Q1="Q1"
JOB_Q2="Q2"
JOB_Q3="Q3"
JOB_Q4="Q4"
JOB_Q5="Q5"
JOB_Q6="Q6"
JOB_Q7="Q7"
JOB_Q8="Q8"
JOB_Q9="Q9"
JOB_Q10="Q10"
JOB_Q11="Q11"
JOB_Q12="Q12"
JOB_Q13="Q13"
JOB_Q14="Q14"
JOB_Q15="Q15"
JOB_Q16="Q16"
JOB_Q17="Q17"
JOB_Q18="Q18"
JOB_Q19="Q19"
JOB_Q20="Q20"
JOB_Q21="Q21"
JOB_Q22="Q22"
JOB_Q23="Q23"
JOB_Q24="Q24"
JOB_Q25="Q25"
JOB_Q26="Q26"
JOB_Q27="Q27"
JOB_Q28="Q28"
JOB_Q29="Q29"
JOB_Q30="Q30"
JOB_Q31="Q31"
JOB_Q32="Q32"
JOB_Q33="Q33"


class Algorithm(Enum):
    TTJHP = "TTJHP"
    LIP = "LIP"
    Yannakakis = "Yannakakis"
    HJ = "HASH_JOIN"


job_bench = {
    # Benchmark name
    BENCHMARK_NAME: JOB,
    # java execution commands files for benchmark
    JAVA_EXECUTION_COMMANDS_FILE: r"job_run_commands.txt",
    # all available benchmarks can be run. Provided list should be
    # some unique identifier to each command in JAVA_EXECUTION_COMMANDS_FILE
    SUPPORT_BENCHMARK_QUERIES: [JOB_Q1, JOB_Q2, JOB_Q3, JOB_Q4, JOB_Q5, JOB_Q6, JOB_Q7, JOB_Q8, JOB_Q9, JOB_Q10, JOB_Q11,
                                JOB_Q12, JOB_Q13, JOB_Q14, JOB_Q15, JOB_Q16, JOB_Q17, JOB_Q18, JOB_Q19, JOB_Q20, JOB_Q21,
                                JOB_Q22, JOB_Q23, JOB_Q24, JOB_Q25, JOB_Q26, JOB_Q27, JOB_Q28, JOB_Q29, JOB_Q30, JOB_Q31,
                                JOB_Q32, JOB_Q33],
    # benchmarks to run
    BENCHMARK_TO_RUN: [],
    # log directory
    LOG_DIR: r"/home/zeyuanhu/projects/challenge-set-gitlab/results/job"
}

find_worst_case_join_ordering_bench = {
    # Benchmark name
    BENCHMARK_NAME: FIND_WORST_CASE_JOIN_ORDERING,
    # java execution commands files for benchmark
    JAVA_EXECUTION_COMMANDS_FILE: r"find_worst_case_join_ordering_commands.txt",
    # all available benchmarks can be run. Provided list should be
    # some unique identifier to each command in JAVA_EXECUTION_COMMANDS_FILE
    SUPPORT_BENCHMARK_QUERIES: [JOB_Q1, JOB_Q2, JOB_Q3, JOB_Q4, JOB_Q5, JOB_Q6, JOB_Q7, JOB_Q8, JOB_Q10, JOB_Q17,
                                JOB_Q16, JOB_Q14, JOB_Q15, JOB_Q18, JOB_Q19, JOB_Q21, JOB_Q23, JOB_Q27, JOB_Q31,
                                JOB_Q32, JOB_Q33],
    # benchmarks to run
    BENCHMARK_TO_RUN: [JOB_Q6],
    # log directory
    LOG_DIR: r"/home/zeyuanhu/projects/challenge-set-gitlab/results/others"
}

pattern_bench = {
    # Benchmark name
    BENCHMARK_NAME: BENCHMARK_PATTERNS,
    # java execution commands files for benchmark
    JAVA_EXECUTION_COMMANDS_FILE: r"bench_pattern.txt",
    # all available benchmarks can be run. Provided list should be
    # some unique identifier to each command in JAVA_EXECUTION_COMMANDS_FILE
    SUPPORT_BENCHMARK_QUERIES: [BENCHMARK_PATTERNS],
    # benchmarks to run
    BENCHMARK_TO_RUN: [],
    # log directory
    LOG_DIR: r"/home/zeyuanhu/projects/challenge-set-gitlab/results/others"
}

gather_additional_stats_bench = {
    BENCHMARK_NAME: GATHER_ADDITIONAL_STATS,
    JAVA_EXECUTION_COMMANDS_FILE: r"bench_gather_additional_stats_job",
    # benchmarks to run
    BENCHMARK_TO_RUN: [JOB_Q1, JOB_Q2, JOB_Q3, JOB_Q4, JOB_Q5, JOB_Q6, JOB_Q7, JOB_Q8, JOB_Q9, JOB_Q10,
                       JOB_Q11, JOB_Q12, JOB_Q13, JOB_Q14, JOB_Q15, JOB_Q16, JOB_Q17, JOB_Q18, JOB_Q19, JOB_Q20,
                       JOB_Q21, JOB_Q22, JOB_Q23, JOB_Q25, JOB_Q26, JOB_Q27, JOB_Q28, JOB_Q30, JOB_Q31, JOB_Q32, JOB_Q33],
    # log directory
    LOG_DIR: r"/home/zeyuanhu/projects/challenge-set-gitlab/results/job/",
    ALGO_TO_RUN: [Algorithm.Yannakakis, Algorithm.LIP, Algorithm.TTJHP]
}