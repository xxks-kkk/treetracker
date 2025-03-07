DATA_SOURCE_CSV = "data_source_csv"
PLOT_FUNCTION = "plot_function"
FIG_SAVE_LOCATION = "fig_save_location"
ALGORITHMS_TO_PLOT = "algorithms_to_plot"
PERCENTILES = "percentiles"
COLUMN_RIGHT_BOUND = "column_left_bound"
ENABLE_NORMALIZATION = "enable_normalization"
ENABLE_SPEEDUP = "enable_speedup"
SORT_DESCENDING_BASED_ON_HJ = "sorted_descending_based_on_hj"

HJ = "HASH_JOIN"
HJ_TTJ_ORDER = "HASH_JOIN_TTJ_ORDER"
HJ_YA_ORDER = "HASH_JOIN_YA_ORDER"
HJ_JOIN_TIME = "HJ_JOIN_TIME"
HJ_REDUC_TIME = "HJ_REDUC_TIME"
TTJ = "TTJHP"
TTJ_JOIN_TIME = "TTJHP_JOIN_TIME"
TTJ_REDUC_TIME = "TTJHP_REDUC_TIME"
TTJ_FIXED_HJ_ORDERING = "TTJ_FIXED_HJ_ORDERING"
TTJ_NO_NG = "TTJHP_NO_NG"
TTJ_BF = "TTJHP_BF"
TTJ_BG = "TTJHP_BG"
TTJ_NO_DP = "TTJHP_NO_DP"
TTJ_VANILLA = "TTJHP_VANILLA"
TTJ_LINEAR = "TTJHP_LINEAR"
LIP = "LIP"
Yannakakis = "Yannakakis"
Yannakakis_JOIN_TIME = "Yannakakis_JOIN_TIME"
Yannakakis_REDUC_TIME = "Yannakakis_REDUC_TIME"
YannakakisB = "YannakakisB"
Yannakakis1Pass = "Yannakakis1Pass"
Yannakakis1Pass_FIXED_HJ_ORDERING = "Yannakakis1Pass_FIXED_HJ_ORDERING"
Yannakakis1Pass_JOIN_TIME = "Yannakakis1Pass_JOIN_TIME"
Yannakakis1Pass_REDUC_TIME = "Yannakakis1Pass_REDUC_TIME"
YannakakisB_JOIN_TIME = "YannakakisB_JOIN_TIME"
YannakakisB_REDUC_TIME = "YannakakisB_REDUC_TIME"
LIP_JOIN_TIME = "LIP_JOIN_TIME"
LIP_REDUC_TIME = "LIP_REDUC_TIME"
YannakakisV = "YannakakisVanilla"
PTO = "PTO"
SQLITE = "SQLITE"
POSTGRES = "POSTGRES"

TTJ_COLOR = '#444444'
TTJ_NO_NG_COLOR = '#6c6564'
TTJ_NO_DP_COLOR = '#938c8a'
TTJ_VANILLA_COLOR = '#545252'
TTJ_REDUC_TIME_COLOR = TTJ_COLOR
TTJ_JOIN_TIME_COLOR = '#7fcca6'

Yannakakis1Pass_COLOR = '#bcbcbc'
Yannakakis1Pass_REDUC_TIME_COLOR = Yannakakis1Pass_COLOR
Yannakakis1Pass_JOIN_TIME_COLOR = '#f9b4c3'

SQLITE_COLOR = "#2C2CFF"
HJ_COLOR = "#999999"

JOB_COLOR = 'blue'
SSB_COLOR = 'orange'
TPC_COLOR = 'green'

DECIMAL_PRECISION = 2

JOB_RELATION_SIZE = {
    "imdb_int.aka_name": 901343,
    "imdb_int.aka_title": 361472,
    "imdb_int.cast_info": 36244344,
    "imdb_int.char_name": 3140339,
    "imdb_int.comp_cast_type": 4,
    "imdb_int.company_name": 234997,
    "imdb_int.company_type": 4,
    "imdb_int.complete_cast": 135086,
    "imdb_int.info_type": 113,
    "imdb_int.keyword": 134170,
    "imdb_int.kind_type": 7,
    "imdb_int.link_type": 18,
    "imdb_int.movie_companies": 2609129,
    "imdb_int.movie_info_idx": 1380035,
    "imdb_int.movie_keyword": 4523930,
    "imdb_int.movie_link": 29997,
    "imdb_int.name": 4167491,
    "imdb_int.role_type": 12,
    "imdb_int.title": 2528312,
    "imdb_int.movie_info": 14835720,
    "imdb_int.person_info": 2963664
}

JOB_SQLITE_ORDERING_RESULTS_INTROW_ON="2024-12-22T01:26:27.330740Z_perf_report.csv"
JOB_POSTGRES_ORDERGING_RESULTS_INTROW_OFF="2024-12-02T00:08:26.123383Z_perf_report.csv"
SSB_SQLITE_ORDERING_RESULTS_INTROW_ON="benchmarkssb-result-2024-07-15t23:56:26.1061benchmarkssb-result-2024-12-09t17:27:00.748485benchmarkssb-result-2024-12-23t17:19:44.362555_perf_report.csv"
SSB_POSTGRES_ORDERING_RESULTS_INTROW_OFF = "benchmarkssbpostgresplansshallow-result-2025-02-14t22:33:39.163677_perf_report.csv"
TPCH_SQLITE_ORDERING_RESULTS_INTROW_ON="2024-12-06T00:39:22.882486Z_perf_report.csv"
TPCH_POSTGRES_ORDERING_RESULTS_INTROW_OFF = "benchmarktpchpostgresplansshallow-result-2025-02-16t22:28:11.550059_perf_report.csv"