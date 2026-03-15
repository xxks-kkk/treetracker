To run the unit tests

```$xslt
$ mvn clean install -P stats -P dev
```

To benchmark performance

```
$ mvn clean install -DskipTests -P prod
```

To enable statistics information collection

```
$ mvn clean install -DskipTests -P stats -P dev
```

Note, for HJ, TTJ, TTJV2 statistics collection, instead of the 
above command, we run

```
$ mvn clean install -DskipTests -P stats
 ```

To enable tracing

```
$ mvn clean install -DskipTests -P dev
$ export treetracker_debug=1
```

# Benchmarking Setup

We demonstrate the setup of SSB benchmark. The rest benchmarks can be set up similarly.

The high-level idea of the setup is that we ingest data into DuckDB,
set up the JDBC connection to DuckDB, and run the benchmark. We
detailed each step below.

## Ingest data into DuckDB

We use [this data generator](https://github.com/eyalroz/ssb-dbgen) to
generate SSB dataset. Follow the instruction of the repo to build data
generator and generate the data via command like `./dbgen -v -s 1`
(scale fator 1 should be good).

For DuckDB, I'm using the version `v0.7.0 f7827396d7` and the
corresponding JDBC driver is bundled as part the codebase, which
doesn't need to be downloaded separately. You can download the required
version of DuckDB [here](https://github.com/duckdb/duckdb/releases/tag/v0.7.0).

You can now ingest the data into duckDB via script `setup-ssb.sh`
inside the repo. Please check `setup-ssb.sh` before running commands:

1. Comment out `psql` commands if you don't have Postgres
   setup. Benchmarking doesn't use Postgres.
2. Examine all the mentioned `.sql` in the script to make sure the
   path to the generated data is correct. Right now, the path is
   hardcoded in the script. For example, in `ssb.sql`, we have
   `'eyalroz-ssb-dbgen/postgres/s1/customer.tbl'`
   in `COPY` command.
   
After sanity checks above, we can run the script, e.g.,

```
$ bin/bash ./treeTracker/treetracker-benchmark/src/main/resources/setup-ssb.sh
```

If the script execution is successful, we finish the ingestion.

## Update JDBC connection

We need to modify `./treeTracker/treetracker-jdbc/src/main/resources/duckdb.properties`
to make sure `db.url` is pointing to the correct duckDB instance. It's
very likely we don't need any modification at all here.

## Run benchmark

We first do 

```
$ mvn clean install -DskipTests -P prod
```

to rebuild the codebase into a production build, which disables
tracing and statistics collection. Then, we run `BenchmarkSSB` file.

To get a sense of no-good list overhead, we can just run 

```
@Param({"TTJHP_NO_DP", "TTJHP_VANILLA"})
```

`TTJHP` is the vanilla TTJ algorithm (`TTJHP_VANILLA`) enhanced with
the two optimizations mentioned in tods.pdf: deletion propagation (dp)
and no-good list. `TTJHP_NO_DP` is the vanilla TTJ + no-good list.

# Data analysis

The resulting of the benchmark is a json file. To analyze it, 
add the file name to **end** of the argument list in 

```
generatePerformanceReport(constructPaths(List.of(
                        // TTHP, Yannakakis1Pass, HJ on fixed HJ ordering from SQLite and share the same join tree
                        Paths.get("hj_ordering_hj", "benchmarkssb-result-2024-07-15t23:56:26.1061.json").toString(),
                        // Run HJ, TTJHP, TTJHP_VANILLA, TTJHP_NO_NG, TTJHP_NO_DP, and Yannakakis1Pass (SF=1)
                        Paths.get("hj_ordering_hj", "benchmarkssb-result-2024-12-09t17:27:00.748485.json").toString(),
                        // Yannakakis1Pass with disablePTOptimizationTrick to true (same across other benchmarks) (SF=1)
                        Paths.get("hj_ordering_hj", "benchmarkssb-result-2024-12-23t17:19:44.362555.json").toString(),
                        // Run HJ, TTJHP, TTJHP_VANILLA, TTJHP_NO_NG, TTJHP_NO_DP, and Yannakakis1Pass (disabling IntRow optimization). Uncomment this
                        // line if we want to use IntRow optimization enabled results. (SF=1) Note Yannakakis1Pass has disablePTOptimizationTrick false.
                        // Paths.get("hj_ordering_hj", "benchmarkssb-result-2024-12-11t01:07:14.181061.json").toString()
                        // TTJHP, TTJHP_NO_NG with extra hash table probe removed from deletion propagation
                        Paths.get("hj_ordering_hj", "benchmarkssb-result-2025-02-24t15:06:20.728865.json").toString()
                ), Benchmarks.SSB),
```

, which appears in `GenerateBenchmarkStatisticsReport.java`. We append
to the end because result appears in the latter overrides the results
appears former. Run `GenerateBenchmarkStatisticsReport.java`, which will produce a csv file that we can use for plotting.

Modify `SSB_SQLITE_ORDERING_RESULTS_INTROW_ON` of `constants.py` in
`scripts/plots/` to pointing to the generated csv. Then, run
`ssb_perf.py` to see the plot.
