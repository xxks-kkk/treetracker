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
import org.zhu45.treetracker.benchmark.job.JOBQueriesFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.JOBQueriesShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.JOBQueriesYannakakis1PassShallow;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class BenchmarkHvTJOBHJOrderingShallow
{
    @State(Scope.Benchmark)
    public static class BenchStateTTJOrdering
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP", "TTJHP_NO_NG"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aOptJoinTreeOptOrderingShallowHJOrdering", "Query10bOptJoinTreeOptOrderingShallowHJOrdering", "Query10cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query11aOptJoinTreeOptOrderingShallowHJOrdering", "Query11bOptJoinTreeOptOrderingShallowHJOrdering", "Query11cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query11dOptJoinTreeOptOrderingShallowHJOrdering", "Query12aOptJoinTreeOptOrderingShallowHJOrdering", "Query12bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query12cOptJoinTreeOptOrderingShallowHJOrdering", "Query13aOptJoinTreeOptOrderingShallowHJOrdering", "Query13bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query13cOptJoinTreeOptOrderingShallowHJOrdering", "Query13dOptJoinTreeOptOrderingShallowHJOrdering", "Query14aOptJoinTreeOptOrderingShallowHJOrdering",
                "Query14bOptJoinTreeOptOrderingShallowHJOrdering", "Query14cOptJoinTreeOptOrderingShallowHJOrdering", "Query15aOptJoinTreeOptOrderingShallowHJOrdering",
                "Query15bOptJoinTreeOptOrderingShallowHJOrdering", "Query15cOptJoinTreeOptOrderingShallowHJOrdering", "Query15dOptJoinTreeOptOrderingShallowHJOrdering",
                "Query16aOptJoinTreeOptOrderingShallowHJOrdering", "Query16bOptJoinTreeOptOrderingShallowHJOrdering", "Query16cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query16dOptJoinTreeOptOrderingShallowHJOrdering", "Query17aOptJoinTreeOptOrderingShallowHJOrdering", "Query17bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query17cOptJoinTreeOptOrderingShallowHJOrdering", "Query17dOptJoinTreeOptOrderingShallowHJOrdering", "Query17eOptJoinTreeOptOrderingShallowHJOrdering",
                "Query17fOptJoinTreeOptOrderingShallowHJOrdering", "Query18aOptJoinTreeOptOrderingShallowHJOrdering", "Query18bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query18cOptJoinTreeOptOrderingShallowHJOrdering", "Query19aOptJoinTreeOptOrderingShallowHJOrdering", "Query19bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query19cOptJoinTreeOptOrderingShallowHJOrdering", "Query19dOptJoinTreeOptOrderingShallowHJOrdering", "Query1aOptJoinTreeOptOrderingShallowHJOrdering",
                "Query1bOptJoinTreeOptOrderingShallowHJOrdering", "Query1cOptJoinTreeOptOrderingShallowHJOrdering", "Query1dOptJoinTreeOptOrderingShallowHJOrdering",
                "Query20aOptJoinTreeOptOrderingShallowHJOrdering", "Query20bOptJoinTreeOptOrderingShallowHJOrdering", "Query20cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query21aOptJoinTreeOptOrderingShallowHJOrdering", "Query21bOptJoinTreeOptOrderingShallowHJOrdering", "Query21cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query22aOptJoinTreeOptOrderingShallowHJOrdering", "Query22bOptJoinTreeOptOrderingShallowHJOrdering", "Query22cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query22dOptJoinTreeOptOrderingShallowHJOrdering", "Query23aOptJoinTreeOptOrderingShallowHJOrdering", "Query23bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query23cOptJoinTreeOptOrderingShallowHJOrdering", "Query24aOptJoinTreeOptOrderingShallowHJOrdering", "Query24bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query25aOptJoinTreeOptOrderingShallowHJOrdering", "Query25bOptJoinTreeOptOrderingShallowHJOrdering", "Query25cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query26aOptJoinTreeOptOrderingShallowHJOrdering", "Query26bOptJoinTreeOptOrderingShallowHJOrdering", "Query26cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query27aOptJoinTreeOptOrderingShallowHJOrdering", "Query27bOptJoinTreeOptOrderingShallowHJOrdering", "Query27cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query28aOptJoinTreeOptOrderingShallowHJOrdering", "Query28bOptJoinTreeOptOrderingShallowHJOrdering", "Query28cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query29aOptJoinTreeOptOrderingShallowHJOrdering", "Query29bOptJoinTreeOptOrderingShallowHJOrdering", "Query29cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query2aOptJoinTreeOptOrderingShallowHJOrdering", "Query2bOptJoinTreeOptOrderingShallowHJOrdering", "Query2cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query2dOptJoinTreeOptOrderingShallowHJOrdering", "Query30aOptJoinTreeOptOrderingShallowHJOrdering", "Query30bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query30cOptJoinTreeOptOrderingShallowHJOrdering", "Query31aOptJoinTreeOptOrderingShallowHJOrdering", "Query31bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query31cOptJoinTreeOptOrderingShallowHJOrdering", "Query32aOptJoinTreeOptOrderingShallowHJOrdering", "Query32bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query33aOptJoinTreeOptOrderingShallowHJOrdering", "Query33bOptJoinTreeOptOrderingShallowHJOrdering", "Query33cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query3aOptJoinTreeOptOrderingShallowHJOrdering", "Query3bOptJoinTreeOptOrderingShallowHJOrdering", "Query3cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query4aOptJoinTreeOptOrderingShallowHJOrdering", "Query4bOptJoinTreeOptOrderingShallowHJOrdering", "Query4cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query5aOptJoinTreeOptOrderingShallowHJOrdering", "Query5bOptJoinTreeOptOrderingShallowHJOrdering", "Query5cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query6aOptJoinTreeOptOrderingShallowHJOrdering", "Query6bOptJoinTreeOptOrderingShallowHJOrdering", "Query6cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query6dOptJoinTreeOptOrderingShallowHJOrdering", "Query6eOptJoinTreeOptOrderingShallowHJOrdering", "Query6fOptJoinTreeOptOrderingShallowHJOrdering",
                "Query7aOptJoinTreeOptOrderingShallowHJOrdering", "Query7bOptJoinTreeOptOrderingShallowHJOrdering", "Query7cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query8aOptJoinTreeOptOrderingShallowHJOrdering", "Query8bOptJoinTreeOptOrderingShallowHJOrdering", "Query8cOptJoinTreeOptOrderingShallowHJOrdering",
                "Query8dOptJoinTreeOptOrderingShallowHJOrdering", "Query9aOptJoinTreeOptOrderingShallowHJOrdering", "Query9bOptJoinTreeOptOrderingShallowHJOrdering",
                "Query9cOptJoinTreeOptOrderingShallowHJOrdering", "Query9dOptJoinTreeOptOrderingShallowHJOrdering"})
        public JOBQueriesShallowHJOrdering jobQueries;
        JoinFragmentType query;

        @Setup(Level.Iteration)
        public void setUp()
        {
            query = queryProvider(joinOperator, jobQueries, duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Iteration)
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
        @Param({"Query10aFindOptJoinTree", "Query10bFindOptJoinTree", "Query10cFindOptJoinTree", "Query11aFindOptJoinTree", "Query11bFindOptJoinTree", "Query11cFindOptJoinTree",
                "Query11dFindOptJoinTree", "Query12aFindOptJoinTree", "Query12bFindOptJoinTree", "Query12cFindOptJoinTree", "Query13aFindOptJoinTree", "Query13bFindOptJoinTree",
                "Query13cFindOptJoinTree", "Query13dFindOptJoinTree", "Query14aFindOptJoinTree", "Query14bFindOptJoinTree", "Query14cFindOptJoinTree", "Query15aFindOptJoinTree",
                "Query15bFindOptJoinTree", "Query15cFindOptJoinTree", "Query15dFindOptJoinTree", "Query16aFindOptJoinTree", "Query16bFindOptJoinTree", "Query16cFindOptJoinTree",
                "Query16dFindOptJoinTree", "Query17aFindOptJoinTree", "Query17bFindOptJoinTree", "Query17cFindOptJoinTree", "Query17dFindOptJoinTree", "Query17eFindOptJoinTree",
                "Query17fFindOptJoinTree", "Query18aFindOptJoinTree", "Query18bFindOptJoinTree", "Query18cFindOptJoinTree", "Query19aFindOptJoinTree", "Query19bFindOptJoinTree",
                "Query19cFindOptJoinTree", "Query19dFindOptJoinTree", "Query1aFindOptJoinTree", "Query1bFindOptJoinTree", "Query1cFindOptJoinTree", "Query1dFindOptJoinTree",
                "Query20aFindOptJoinTree", "Query20bFindOptJoinTree", "Query20cFindOptJoinTree", "Query21aFindOptJoinTree", "Query21bFindOptJoinTree", "Query21cFindOptJoinTree",
                "Query22aFindOptJoinTree", "Query22bFindOptJoinTree", "Query22cFindOptJoinTree", "Query22dFindOptJoinTree", "Query23aFindOptJoinTree", "Query23bFindOptJoinTree",
                "Query23cFindOptJoinTree", "Query24aFindOptJoinTree", "Query24bFindOptJoinTree", "Query25aFindOptJoinTree", "Query25bFindOptJoinTree", "Query25cFindOptJoinTree",
                "Query26aFindOptJoinTree", "Query26bFindOptJoinTree", "Query26cFindOptJoinTree", "Query27aFindOptJoinTree", "Query27bFindOptJoinTree", "Query27cFindOptJoinTree",
                "Query28aFindOptJoinTree", "Query28bFindOptJoinTree", "Query28cFindOptJoinTree", "Query29aFindOptJoinTree", "Query29bFindOptJoinTree", "Query29cFindOptJoinTree",
                "Query2aFindOptJoinTree", "Query2bFindOptJoinTree", "Query2cFindOptJoinTree", "Query2dFindOptJoinTree", "Query30aFindOptJoinTree", "Query30bFindOptJoinTree",
                "Query30cFindOptJoinTree", "Query31aFindOptJoinTree", "Query31bFindOptJoinTree", "Query31cFindOptJoinTree", "Query32aFindOptJoinTree", "Query32bFindOptJoinTree",
                "Query33aFindOptJoinTree", "Query33bFindOptJoinTree", "Query33cFindOptJoinTree", "Query3aFindOptJoinTree", "Query3bFindOptJoinTree", "Query3cFindOptJoinTree",
                "Query4aFindOptJoinTree", "Query4bFindOptJoinTree", "Query4cFindOptJoinTree", "Query5aFindOptJoinTree", "Query5bFindOptJoinTree", "Query5cFindOptJoinTree",
                "Query6aFindOptJoinTree", "Query6bFindOptJoinTree", "Query6cFindOptJoinTree", "Query6dFindOptJoinTree", "Query6eFindOptJoinTree", "Query6fFindOptJoinTree",
                "Query7aFindOptJoinTree", "Query7bFindOptJoinTree", "Query7cFindOptJoinTree", "Query8aFindOptJoinTree", "Query8bFindOptJoinTree", "Query8cFindOptJoinTree",
                "Query8dFindOptJoinTree", "Query9aFindOptJoinTree", "Query9bFindOptJoinTree", "Query9cFindOptJoinTree", "Query9dFindOptJoinTree"})
        public JOBQueriesFindOptJoinTree jobQueries;
        JoinFragmentType query;

        @Setup(Level.Iteration)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    jobQueries,
                    List.of(),
                    duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Iteration)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchStateYannakakis1Pass
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis1Pass"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query10bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query10cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query11aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query11bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query11cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query11dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query12aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query12bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query12cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query13aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query13bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query13cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query13dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query14aOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query14bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query14cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query15aOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query15bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query15cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query15dOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query16aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query16bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query16cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query16dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query17aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query17bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query17cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query17dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query17eOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query17fOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query18aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query18bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query18cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query19aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query19bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query19cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query19dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query1aOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query1bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query1cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query1dOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query20aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query20bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query20cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query21aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query21bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query21cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query22aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query22bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query22cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query22dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query23aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query23bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query23cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query24aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query24bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query25aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query25bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query25cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query26aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query26bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query26cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query27aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query27bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query27cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query28aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query28bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query28cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query29aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query29bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query29cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query2aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query2bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query2cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query2dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query30aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query30bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query30cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query31aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query31bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query31cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query32aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query32bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query33aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query33bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query33cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query3aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query3bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query3cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query4aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query4bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query4cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query5aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query5bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query5cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query6aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query6bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query6cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query6dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query6eOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query6fOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query7aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query7bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query7cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query8aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query8bOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query8cOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query8dOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query9aOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query9bOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query9cOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query9dOptJoinTreeOptOrderingY1PShallowHJOrdering"})
        public JOBQueriesYannakakis1PassShallow jobQueries;
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
    public void benchTTJ(BenchStateTTJOrdering state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, JOB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchHJ(BenchStateHJ state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, JOB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchYannakakis1Pass(BenchStateYannakakis1Pass state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, JOB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH);
        Thread.sleep(2000);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkHvTJOBHJOrderingShallow.class, JOB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH).run();
    }
}
