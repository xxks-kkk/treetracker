package org.zhu45.treetracker.benchmark.hvt;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.zhu45.treetracker.benchmark.job.JOBQueriesAutoGen;
import org.zhu45.treetracker.benchmark.job.JOBQueriesFindOptJoinTree;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 0, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class BenchmarkHvTJOB
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aOptJoinTreeOptOrdering", "Query10bOptJoinTreeOptOrdering", "Query10cOptJoinTreeOptOrdering", "Query11aOptJoinTreeOptOrdering",
                "Query11bOptJoinTreeOptOrdering", "Query11cOptJoinTreeOptOrdering", "Query11dOptJoinTreeOptOrdering", "Query12aOptJoinTreeOptOrdering",
                "Query12bOptJoinTreeOptOrdering", "Query12cOptJoinTreeOptOrdering", "Query13aOptJoinTreeOptOrdering", "Query13bOptJoinTreeOptOrdering",
                "Query13cOptJoinTreeOptOrdering", "Query13dOptJoinTreeOptOrdering", "Query14aOptJoinTreeOptOrdering", "Query14bOptJoinTreeOptOrdering",
                "Query14cOptJoinTreeOptOrdering", "Query15aOptJoinTreeOptOrdering", "Query15bOptJoinTreeOptOrdering", "Query15cOptJoinTreeOptOrdering",
                "Query15dOptJoinTreeOptOrdering", "Query16aOptJoinTreeOptOrdering", "Query16bOptJoinTreeOptOrdering", "Query16cOptJoinTreeOptOrdering",
                "Query16dOptJoinTreeOptOrdering", "Query17aOptJoinTreeOptOrdering", "Query17bOptJoinTreeOptOrdering", "Query17cOptJoinTreeOptOrdering",
                "Query17dOptJoinTreeOptOrdering", "Query17eOptJoinTreeOptOrdering", "Query17fOptJoinTreeOptOrdering", "Query18aOptJoinTreeOptOrdering",
                "Query18bOptJoinTreeOptOrdering", "Query18cOptJoinTreeOptOrdering", "Query19aOptJoinTreeOptOrdering", "Query19bOptJoinTreeOptOrdering",
                "Query19cOptJoinTreeOptOrdering", "Query19dOptJoinTreeOptOrdering", "Query1aOptJoinTreeOptOrdering", "Query1bOptJoinTreeOptOrdering",
                "Query1cOptJoinTreeOptOrdering", "Query1dOptJoinTreeOptOrdering", "Query20aOptJoinTreeOptOrdering", "Query20bOptJoinTreeOptOrdering",
                "Query20cOptJoinTreeOptOrdering", "Query21aOptJoinTreeOptOrdering", "Query21bOptJoinTreeOptOrdering", "Query21cOptJoinTreeOptOrdering",
                "Query22aOptJoinTreeOptOrdering", "Query22bOptJoinTreeOptOrdering", "Query22cOptJoinTreeOptOrdering", "Query22dOptJoinTreeOptOrdering",
                "Query23aOptJoinTreeOptOrdering", "Query23bOptJoinTreeOptOrdering", "Query23cOptJoinTreeOptOrdering", "Query24aOptJoinTreeOptOrdering",
                "Query24bOptJoinTreeOptOrdering", "Query25aOptJoinTreeOptOrdering", "Query25bOptJoinTreeOptOrdering", "Query25cOptJoinTreeOptOrdering",
                "Query26aOptJoinTreeOptOrdering", "Query26bOptJoinTreeOptOrdering", "Query26cOptJoinTreeOptOrdering", "Query27aOptJoinTreeOptOrdering",
                "Query27bOptJoinTreeOptOrdering", "Query27cOptJoinTreeOptOrdering", "Query28aOptJoinTreeOptOrdering", "Query28bOptJoinTreeOptOrdering",
                "Query28cOptJoinTreeOptOrdering", "Query29aOptJoinTreeOptOrdering", "Query29bOptJoinTreeOptOrdering", "Query29cOptJoinTreeOptOrdering",
                "Query2aOptJoinTreeOptOrdering", "Query2bOptJoinTreeOptOrdering", "Query2cOptJoinTreeOptOrdering", "Query2dOptJoinTreeOptOrdering",
                "Query30aOptJoinTreeOptOrdering", "Query30bOptJoinTreeOptOrdering", "Query30cOptJoinTreeOptOrdering", "Query31aOptJoinTreeOptOrdering",
                "Query31bOptJoinTreeOptOrdering", "Query31cOptJoinTreeOptOrdering", "Query32aOptJoinTreeOptOrdering", "Query32bOptJoinTreeOptOrdering",
                "Query33aOptJoinTreeOptOrdering", "Query33bOptJoinTreeOptOrdering", "Query33cOptJoinTreeOptOrdering", "Query3aOptJoinTreeOptOrdering",
                "Query3bOptJoinTreeOptOrdering", "Query3cOptJoinTreeOptOrdering", "Query4aOptJoinTreeOptOrdering", "Query4bOptJoinTreeOptOrdering", "Query4cOptJoinTreeOptOrdering",
                "Query5aOptJoinTreeOptOrdering", "Query5bOptJoinTreeOptOrdering", "Query5cOptJoinTreeOptOrdering", "Query6aOptJoinTreeOptOrdering", "Query6bOptJoinTreeOptOrdering",
                "Query6cOptJoinTreeOptOrdering", "Query6dOptJoinTreeOptOrdering", "Query6eOptJoinTreeOptOrdering", "Query6fOptJoinTreeOptOrdering", "Query7aOptJoinTreeOptOrdering",
                "Query7bOptJoinTreeOptOrdering", "Query7cOptJoinTreeOptOrdering", "Query8aOptJoinTreeOptOrdering", "Query8bOptJoinTreeOptOrdering", "Query8cOptJoinTreeOptOrdering",
                "Query8dOptJoinTreeOptOrdering", "Query9aOptJoinTreeOptOrdering", "Query9bOptJoinTreeOptOrdering", "Query9cOptJoinTreeOptOrdering",
                "Query9dOptJoinTreeOptOrdering"})
        public JOBQueriesAutoGen jobQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator, jobQueries, duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchStateHJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"HASH_JOIN"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aFindOptJoinTree", "Query10bFindOptJoinTree", "Query10cFindOptJoinTree", "Query11aFindOptJoinTree", "Query11bFindOptJoinTree", "Query11cFindOptJoinTree", "Query11dFindOptJoinTree", "Query12aFindOptJoinTree", "Query12bFindOptJoinTree", "Query12cFindOptJoinTree", "Query13aFindOptJoinTree", "Query13bFindOptJoinTree", "Query13cFindOptJoinTree", "Query13dFindOptJoinTree", "Query14aFindOptJoinTree", "Query14bFindOptJoinTree", "Query14cFindOptJoinTree", "Query15aFindOptJoinTree", "Query15bFindOptJoinTree", "Query15cFindOptJoinTree", "Query15dFindOptJoinTree", "Query16aFindOptJoinTree", "Query16bFindOptJoinTree", "Query16cFindOptJoinTree", "Query16dFindOptJoinTree", "Query17aFindOptJoinTree", "Query17bFindOptJoinTree", "Query17cFindOptJoinTree", "Query17dFindOptJoinTree", "Query17eFindOptJoinTree", "Query17fFindOptJoinTree", "Query18aFindOptJoinTree", "Query18bFindOptJoinTree", "Query18cFindOptJoinTree", "Query19aFindOptJoinTree", "Query19bFindOptJoinTree", "Query19cFindOptJoinTree", "Query19dFindOptJoinTree", "Query1aFindOptJoinTree", "Query1bFindOptJoinTree", "Query1cFindOptJoinTree", "Query1dFindOptJoinTree", "Query20aFindOptJoinTree", "Query20bFindOptJoinTree", "Query20cFindOptJoinTree", "Query21aFindOptJoinTree", "Query21bFindOptJoinTree", "Query21cFindOptJoinTree", "Query22aFindOptJoinTree", "Query22bFindOptJoinTree", "Query22cFindOptJoinTree", "Query22dFindOptJoinTree", "Query23aFindOptJoinTree", "Query23bFindOptJoinTree", "Query23cFindOptJoinTree", "Query24aFindOptJoinTree", "Query24bFindOptJoinTree", "Query25aFindOptJoinTree", "Query25bFindOptJoinTree", "Query25cFindOptJoinTree", "Query26aFindOptJoinTree", "Query26bFindOptJoinTree", "Query26cFindOptJoinTree", "Query27aFindOptJoinTree", "Query27bFindOptJoinTree", "Query27cFindOptJoinTree", "Query28aFindOptJoinTree", "Query28bFindOptJoinTree", "Query28cFindOptJoinTree", "Query29aFindOptJoinTree", "Query29bFindOptJoinTree", "Query29cFindOptJoinTree", "Query2aFindOptJoinTree", "Query2bFindOptJoinTree", "Query2cFindOptJoinTree", "Query2dFindOptJoinTree", "Query30aFindOptJoinTree", "Query30bFindOptJoinTree", "Query30cFindOptJoinTree", "Query31aFindOptJoinTree", "Query31bFindOptJoinTree", "Query31cFindOptJoinTree", "Query32aFindOptJoinTree", "Query32bFindOptJoinTree", "Query33aFindOptJoinTree", "Query33bFindOptJoinTree", "Query33cFindOptJoinTree", "Query3aFindOptJoinTree", "Query3bFindOptJoinTree", "Query3cFindOptJoinTree", "Query4aFindOptJoinTree", "Query4bFindOptJoinTree", "Query4cFindOptJoinTree", "Query5aFindOptJoinTree", "Query5bFindOptJoinTree", "Query5cFindOptJoinTree", "Query6aFindOptJoinTree", "Query6bFindOptJoinTree", "Query6cFindOptJoinTree", "Query6dFindOptJoinTree", "Query6eFindOptJoinTree", "Query6fFindOptJoinTree", "Query7aFindOptJoinTree", "Query7bFindOptJoinTree", "Query7cFindOptJoinTree", "Query8aFindOptJoinTree", "Query8bFindOptJoinTree", "Query8cFindOptJoinTree", "Query8dFindOptJoinTree", "Query9aFindOptJoinTree", "Query9bFindOptJoinTree", "Query9cFindOptJoinTree", "Query9dFindOptJoinTree"})
        public JOBQueriesFindOptJoinTree jobQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    jobQueries,
                    List.of(),
                    duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @Benchmark
    public void bench(BenchState state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchHJ(BenchStateHJ state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkHvTJOB.class, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH).run();
    }
}
