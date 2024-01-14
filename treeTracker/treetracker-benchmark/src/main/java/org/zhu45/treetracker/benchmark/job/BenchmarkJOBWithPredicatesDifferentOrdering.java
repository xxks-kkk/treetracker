package org.zhu45.treetracker.benchmark.job;

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
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 10, warmups = 5, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BenchmarkJOBWithPredicatesDifferentOrdering
{
    @State(Scope.Benchmark)
    public static class BenchStateTTJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aOptJoinTreeOptOrdering", "Query10bOptJoinTreeOptOrdering", "Query10cOptJoinTreeOptOrdering", "Query11aOptJoinTreeOptOrdering", "Query11bOptJoinTreeOptOrdering", "Query11cOptJoinTreeOptOrdering", "Query11dOptJoinTreeOptOrdering", "Query12aOptJoinTreeOptOrdering", "Query12bOptJoinTreeOptOrdering", "Query12cOptJoinTreeOptOrdering", "Query13aOptJoinTreeOptOrdering", "Query13bOptJoinTreeOptOrdering", "Query13cOptJoinTreeOptOrdering", "Query13dOptJoinTreeOptOrdering", "Query14aOptJoinTreeOptOrdering", "Query14bOptJoinTreeOptOrdering", "Query14cOptJoinTreeOptOrdering", "Query15aOptJoinTreeOptOrdering", "Query15bOptJoinTreeOptOrdering", "Query15cOptJoinTreeOptOrdering", "Query15dOptJoinTreeOptOrdering", "Query16aOptJoinTreeOptOrdering", "Query16bOptJoinTreeOptOrdering", "Query16cOptJoinTreeOptOrdering", "Query16dOptJoinTreeOptOrdering", "Query17aOptJoinTreeOptOrdering", "Query17bOptJoinTreeOptOrdering", "Query17cOptJoinTreeOptOrdering", "Query17dOptJoinTreeOptOrdering", "Query17eOptJoinTreeOptOrdering", "Query17fOptJoinTreeOptOrdering", "Query18aOptJoinTreeOptOrdering", "Query18bOptJoinTreeOptOrdering", "Query18cOptJoinTreeOptOrdering", "Query19aOptJoinTreeOptOrdering", "Query19bOptJoinTreeOptOrdering", "Query19cOptJoinTreeOptOrdering", "Query19dOptJoinTreeOptOrdering", "Query1aOptJoinTreeOptOrdering", "Query1bOptJoinTreeOptOrdering", "Query1cOptJoinTreeOptOrdering", "Query1dOptJoinTreeOptOrdering", "Query20aOptJoinTreeOptOrdering", "Query20bOptJoinTreeOptOrdering", "Query20cOptJoinTreeOptOrdering", "Query21aOptJoinTreeOptOrdering", "Query21bOptJoinTreeOptOrdering", "Query21cOptJoinTreeOptOrdering", "Query22aOptJoinTreeOptOrdering", "Query22bOptJoinTreeOptOrdering", "Query22cOptJoinTreeOptOrdering", "Query22dOptJoinTreeOptOrdering", "Query23aOptJoinTreeOptOrdering", "Query23bOptJoinTreeOptOrdering", "Query23cOptJoinTreeOptOrdering", "Query24aOptJoinTreeOptOrdering", "Query24bOptJoinTreeOptOrdering", "Query25aOptJoinTreeOptOrdering", "Query25bOptJoinTreeOptOrdering", "Query25cOptJoinTreeOptOrdering", "Query26aOptJoinTreeOptOrdering", "Query26bOptJoinTreeOptOrdering", "Query26cOptJoinTreeOptOrdering", "Query27aOptJoinTreeOptOrdering", "Query27bOptJoinTreeOptOrdering", "Query27cOptJoinTreeOptOrdering", "Query28aOptJoinTreeOptOrdering", "Query28bOptJoinTreeOptOrdering", "Query28cOptJoinTreeOptOrdering", "Query29aOptJoinTreeOptOrdering", "Query29bOptJoinTreeOptOrdering", "Query29cOptJoinTreeOptOrdering", "Query2aOptJoinTreeOptOrdering", "Query2bOptJoinTreeOptOrdering", "Query2cOptJoinTreeOptOrdering", "Query2dOptJoinTreeOptOrdering", "Query30aOptJoinTreeOptOrdering", "Query30bOptJoinTreeOptOrdering", "Query30cOptJoinTreeOptOrdering", "Query31aOptJoinTreeOptOrdering", "Query31bOptJoinTreeOptOrdering", "Query31cOptJoinTreeOptOrdering", "Query32aOptJoinTreeOptOrdering", "Query32bOptJoinTreeOptOrdering", "Query33aOptJoinTreeOptOrdering", "Query33bOptJoinTreeOptOrdering", "Query33cOptJoinTreeOptOrdering", "Query3aOptJoinTreeOptOrdering", "Query3bOptJoinTreeOptOrdering", "Query3cOptJoinTreeOptOrdering", "Query4aOptJoinTreeOptOrdering", "Query4bOptJoinTreeOptOrdering", "Query4cOptJoinTreeOptOrdering", "Query5aOptJoinTreeOptOrdering", "Query5bOptJoinTreeOptOrdering", "Query5cOptJoinTreeOptOrdering", "Query6aOptJoinTreeOptOrdering", "Query6bOptJoinTreeOptOrdering", "Query6cOptJoinTreeOptOrdering", "Query6dOptJoinTreeOptOrdering", "Query6eOptJoinTreeOptOrdering", "Query6fOptJoinTreeOptOrdering", "Query7aOptJoinTreeOptOrdering", "Query7bOptJoinTreeOptOrdering", "Query7cOptJoinTreeOptOrdering", "Query8aOptJoinTreeOptOrdering", "Query8bOptJoinTreeOptOrdering", "Query8cOptJoinTreeOptOrdering", "Query8dOptJoinTreeOptOrdering", "Query9aOptJoinTreeOptOrdering", "Query9bOptJoinTreeOptOrdering", "Query9cOptJoinTreeOptOrdering", "Query9dOptJoinTreeOptOrdering"})
        public JOBQueriesAutoGen jobQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator, jobQueries,
                    List.of(),
                    duckDBJdbcClientSupplier.get());
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

    @State(Scope.Benchmark)
    public static class BenchStateYannakakis
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aOptJoinTreeOptOrderingY", "Query10bOptJoinTreeOptOrderingY", "Query10cOptJoinTreeOptOrderingY", "Query11aOptJoinTreeOptOrderingY", "Query11bOptJoinTreeOptOrderingY", "Query11cOptJoinTreeOptOrderingY", "Query11dOptJoinTreeOptOrderingY", "Query12aOptJoinTreeOptOrderingY", "Query12bOptJoinTreeOptOrderingY", "Query12cOptJoinTreeOptOrderingY", "Query13aOptJoinTreeOptOrderingY", "Query13bOptJoinTreeOptOrderingY", "Query13cOptJoinTreeOptOrderingY", "Query13dOptJoinTreeOptOrderingY", "Query14aOptJoinTreeOptOrderingY", "Query14bOptJoinTreeOptOrderingY", "Query14cOptJoinTreeOptOrderingY", "Query15aOptJoinTreeOptOrderingY", "Query15bOptJoinTreeOptOrderingY", "Query15cOptJoinTreeOptOrderingY", "Query15dOptJoinTreeOptOrderingY", "Query16aOptJoinTreeOptOrderingY", "Query16bOptJoinTreeOptOrderingY", "Query16cOptJoinTreeOptOrderingY", "Query16dOptJoinTreeOptOrderingY", "Query17aOptJoinTreeOptOrderingY", "Query17bOptJoinTreeOptOrderingY", "Query17cOptJoinTreeOptOrderingY", "Query17dOptJoinTreeOptOrderingY", "Query17eOptJoinTreeOptOrderingY", "Query17fOptJoinTreeOptOrderingY", "Query18aOptJoinTreeOptOrderingY", "Query18bOptJoinTreeOptOrderingY", "Query18cOptJoinTreeOptOrderingY", "Query19aOptJoinTreeOptOrderingY", "Query19bOptJoinTreeOptOrderingY", "Query19cOptJoinTreeOptOrderingY", "Query19dOptJoinTreeOptOrderingY", "Query1aOptJoinTreeOptOrderingY", "Query1bOptJoinTreeOptOrderingY", "Query1cOptJoinTreeOptOrderingY", "Query1dOptJoinTreeOptOrderingY", "Query20aOptJoinTreeOptOrderingY", "Query20bOptJoinTreeOptOrderingY", "Query20cOptJoinTreeOptOrderingY", "Query21aOptJoinTreeOptOrderingY", "Query21bOptJoinTreeOptOrderingY", "Query21cOptJoinTreeOptOrderingY", "Query22aOptJoinTreeOptOrderingY", "Query22bOptJoinTreeOptOrderingY", "Query22cOptJoinTreeOptOrderingY", "Query22dOptJoinTreeOptOrderingY", "Query23aOptJoinTreeOptOrderingY", "Query23bOptJoinTreeOptOrderingY", "Query23cOptJoinTreeOptOrderingY", "Query24aOptJoinTreeOptOrderingY", "Query24bOptJoinTreeOptOrderingY", "Query25aOptJoinTreeOptOrderingY", "Query25bOptJoinTreeOptOrderingY", "Query25cOptJoinTreeOptOrderingY", "Query26aOptJoinTreeOptOrderingY", "Query26bOptJoinTreeOptOrderingY", "Query26cOptJoinTreeOptOrderingY", "Query27aOptJoinTreeOptOrderingY", "Query27bOptJoinTreeOptOrderingY", "Query27cOptJoinTreeOptOrderingY", "Query28aOptJoinTreeOptOrderingY", "Query28bOptJoinTreeOptOrderingY", "Query28cOptJoinTreeOptOrderingY", "Query29aOptJoinTreeOptOrderingY", "Query29bOptJoinTreeOptOrderingY", "Query29cOptJoinTreeOptOrderingY", "Query2aOptJoinTreeOptOrderingY", "Query2bOptJoinTreeOptOrderingY", "Query2cOptJoinTreeOptOrderingY", "Query2dOptJoinTreeOptOrderingY", "Query30aOptJoinTreeOptOrderingY", "Query30bOptJoinTreeOptOrderingY", "Query30cOptJoinTreeOptOrderingY", "Query31aOptJoinTreeOptOrderingY", "Query31bOptJoinTreeOptOrderingY", "Query31cOptJoinTreeOptOrderingY", "Query32aOptJoinTreeOptOrderingY", "Query32bOptJoinTreeOptOrderingY", "Query33aOptJoinTreeOptOrderingY", "Query33bOptJoinTreeOptOrderingY", "Query33cOptJoinTreeOptOrderingY", "Query3aOptJoinTreeOptOrderingY", "Query3bOptJoinTreeOptOrderingY", "Query3cOptJoinTreeOptOrderingY", "Query4aOptJoinTreeOptOrderingY", "Query4bOptJoinTreeOptOrderingY", "Query4cOptJoinTreeOptOrderingY", "Query5aOptJoinTreeOptOrderingY", "Query5bOptJoinTreeOptOrderingY", "Query5cOptJoinTreeOptOrderingY", "Query6aOptJoinTreeOptOrderingY", "Query6bOptJoinTreeOptOrderingY", "Query6cOptJoinTreeOptOrderingY", "Query6dOptJoinTreeOptOrderingY", "Query6eOptJoinTreeOptOrderingY", "Query6fOptJoinTreeOptOrderingY", "Query7aOptJoinTreeOptOrderingY", "Query7bOptJoinTreeOptOrderingY", "Query7cOptJoinTreeOptOrderingY", "Query8aOptJoinTreeOptOrderingY", "Query8bOptJoinTreeOptOrderingY", "Query8cOptJoinTreeOptOrderingY", "Query8dOptJoinTreeOptOrderingY", "Query9aOptJoinTreeOptOrderingY", "Query9bOptJoinTreeOptOrderingY", "Query9cOptJoinTreeOptOrderingY", "Query9dOptJoinTreeOptOrderingY"})
        public JOBQueriesYannakakis jobQueries;
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

    @State(Scope.Benchmark)
    public static class BenchStateYannakakisB
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"YannakakisB"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aOptJoinTreeOptOrderingYB", "Query10bOptJoinTreeOptOrderingYB", "Query10cOptJoinTreeOptOrderingYB", "Query11aOptJoinTreeOptOrderingYB", "Query11bOptJoinTreeOptOrderingYB", "Query11cOptJoinTreeOptOrderingYB", "Query11dOptJoinTreeOptOrderingYB", "Query12aOptJoinTreeOptOrderingYB", "Query12bOptJoinTreeOptOrderingYB", "Query12cOptJoinTreeOptOrderingYB", "Query13aOptJoinTreeOptOrderingYB", "Query13bOptJoinTreeOptOrderingYB", "Query13cOptJoinTreeOptOrderingYB", "Query13dOptJoinTreeOptOrderingYB", "Query14aOptJoinTreeOptOrderingYB", "Query14bOptJoinTreeOptOrderingYB", "Query14cOptJoinTreeOptOrderingYB", "Query15aOptJoinTreeOptOrderingYB", "Query15bOptJoinTreeOptOrderingYB", "Query15cOptJoinTreeOptOrderingYB", "Query15dOptJoinTreeOptOrderingYB", "Query16aOptJoinTreeOptOrderingYB", "Query16bOptJoinTreeOptOrderingYB", "Query16cOptJoinTreeOptOrderingYB", "Query16dOptJoinTreeOptOrderingYB", "Query17aOptJoinTreeOptOrderingYB", "Query17bOptJoinTreeOptOrderingYB", "Query17cOptJoinTreeOptOrderingYB", "Query17dOptJoinTreeOptOrderingYB", "Query17eOptJoinTreeOptOrderingYB", "Query17fOptJoinTreeOptOrderingYB", "Query18aOptJoinTreeOptOrderingYB", "Query18bOptJoinTreeOptOrderingYB", "Query18cOptJoinTreeOptOrderingYB", "Query19aOptJoinTreeOptOrderingYB", "Query19bOptJoinTreeOptOrderingYB", "Query19cOptJoinTreeOptOrderingYB", "Query19dOptJoinTreeOptOrderingYB", "Query1aOptJoinTreeOptOrderingYB", "Query1bOptJoinTreeOptOrderingYB", "Query1cOptJoinTreeOptOrderingYB", "Query1dOptJoinTreeOptOrderingYB", "Query20aOptJoinTreeOptOrderingYB", "Query20bOptJoinTreeOptOrderingYB", "Query20cOptJoinTreeOptOrderingYB", "Query21aOptJoinTreeOptOrderingYB", "Query21bOptJoinTreeOptOrderingYB", "Query21cOptJoinTreeOptOrderingYB", "Query22aOptJoinTreeOptOrderingYB", "Query22bOptJoinTreeOptOrderingYB", "Query22cOptJoinTreeOptOrderingYB", "Query22dOptJoinTreeOptOrderingYB", "Query23aOptJoinTreeOptOrderingYB", "Query23bOptJoinTreeOptOrderingYB", "Query23cOptJoinTreeOptOrderingYB", "Query24aOptJoinTreeOptOrderingYB", "Query24bOptJoinTreeOptOrderingYB", "Query25aOptJoinTreeOptOrderingYB", "Query25bOptJoinTreeOptOrderingYB", "Query25cOptJoinTreeOptOrderingYB", "Query26aOptJoinTreeOptOrderingYB", "Query26bOptJoinTreeOptOrderingYB", "Query26cOptJoinTreeOptOrderingYB", "Query27aOptJoinTreeOptOrderingYB", "Query27bOptJoinTreeOptOrderingYB", "Query27cOptJoinTreeOptOrderingYB", "Query28aOptJoinTreeOptOrderingYB", "Query28bOptJoinTreeOptOrderingYB", "Query28cOptJoinTreeOptOrderingYB", "Query29aOptJoinTreeOptOrderingYB", "Query29bOptJoinTreeOptOrderingYB", "Query29cOptJoinTreeOptOrderingYB", "Query2aOptJoinTreeOptOrderingYB", "Query2bOptJoinTreeOptOrderingYB", "Query2cOptJoinTreeOptOrderingYB", "Query2dOptJoinTreeOptOrderingYB", "Query30aOptJoinTreeOptOrderingYB", "Query30bOptJoinTreeOptOrderingYB", "Query30cOptJoinTreeOptOrderingYB", "Query31aOptJoinTreeOptOrderingYB", "Query31bOptJoinTreeOptOrderingYB", "Query31cOptJoinTreeOptOrderingYB", "Query32aOptJoinTreeOptOrderingYB", "Query32bOptJoinTreeOptOrderingYB", "Query33aOptJoinTreeOptOrderingYB", "Query33bOptJoinTreeOptOrderingYB", "Query33cOptJoinTreeOptOrderingYB", "Query3aOptJoinTreeOptOrderingYB", "Query3bOptJoinTreeOptOrderingYB", "Query3cOptJoinTreeOptOrderingYB", "Query4aOptJoinTreeOptOrderingYB", "Query4bOptJoinTreeOptOrderingYB", "Query4cOptJoinTreeOptOrderingYB", "Query5aOptJoinTreeOptOrderingYB", "Query5bOptJoinTreeOptOrderingYB", "Query5cOptJoinTreeOptOrderingYB", "Query6aOptJoinTreeOptOrderingYB", "Query6bOptJoinTreeOptOrderingYB", "Query6cOptJoinTreeOptOrderingYB", "Query6dOptJoinTreeOptOrderingYB", "Query6eOptJoinTreeOptOrderingYB", "Query6fOptJoinTreeOptOrderingYB", "Query7aOptJoinTreeOptOrderingYB", "Query7bOptJoinTreeOptOrderingYB", "Query7cOptJoinTreeOptOrderingYB", "Query8aOptJoinTreeOptOrderingYB", "Query8bOptJoinTreeOptOrderingYB", "Query8cOptJoinTreeOptOrderingYB", "Query8dOptJoinTreeOptOrderingYB", "Query9aOptJoinTreeOptOrderingYB", "Query9bOptJoinTreeOptOrderingYB", "Query9cOptJoinTreeOptOrderingYB", "Query9dOptJoinTreeOptOrderingYB"})
        public JOBQueriesYannakakisB jobQueries;
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
    public void benchTTJ(BenchStateTTJ state)
    {
        state.query.eval();
    }

    @Benchmark
    public void benchHJ(BenchStateHJ state)
    {
        state.query.eval();
    }

    @Benchmark
    public void benchYannakakis(BenchStateYannakakis state)
    {
        state.query.eval();
    }

    @Benchmark
    public void benchYannakakisB(BenchStateYannakakisB state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkJOBWithPredicatesDifferentOrdering.class,
                JOB_WITH_PREDICATES_RESULT_STORED_PATH).run();
    }
}
