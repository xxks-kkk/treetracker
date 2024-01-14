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
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 10, warmups = 5, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BenchmarkJOBWithPredicatesFixedHJOrdering
{
    @State(Scope.Benchmark)
    public static class BenchStateTTJFixedHJOrdering
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10aOptJoinTreeOptOrderingFixedHJOrdering", "Query10bOptJoinTreeOptOrderingFixedHJOrdering", "Query10cOptJoinTreeOptOrderingFixedHJOrdering", "Query11aOptJoinTreeOptOrderingFixedHJOrdering", "Query11bOptJoinTreeOptOrderingFixedHJOrdering", "Query11cOptJoinTreeOptOrderingFixedHJOrdering", "Query11dOptJoinTreeOptOrderingFixedHJOrdering", "Query12aOptJoinTreeOptOrderingFixedHJOrdering", "Query12bOptJoinTreeOptOrderingFixedHJOrdering", "Query12cOptJoinTreeOptOrderingFixedHJOrdering", "Query13aOptJoinTreeOptOrderingFixedHJOrdering", "Query13bOptJoinTreeOptOrderingFixedHJOrdering", "Query13cOptJoinTreeOptOrderingFixedHJOrdering", "Query13dOptJoinTreeOptOrderingFixedHJOrdering", "Query14aOptJoinTreeOptOrderingFixedHJOrdering", "Query14bOptJoinTreeOptOrderingFixedHJOrdering", "Query14cOptJoinTreeOptOrderingFixedHJOrdering", "Query15aOptJoinTreeOptOrderingFixedHJOrdering", "Query15bOptJoinTreeOptOrderingFixedHJOrdering", "Query15cOptJoinTreeOptOrderingFixedHJOrdering", "Query15dOptJoinTreeOptOrderingFixedHJOrdering", "Query16aOptJoinTreeOptOrderingFixedHJOrdering", "Query16bOptJoinTreeOptOrderingFixedHJOrdering", "Query16cOptJoinTreeOptOrderingFixedHJOrdering", "Query16dOptJoinTreeOptOrderingFixedHJOrdering", "Query17aOptJoinTreeOptOrderingFixedHJOrdering", "Query17bOptJoinTreeOptOrderingFixedHJOrdering", "Query17cOptJoinTreeOptOrderingFixedHJOrdering", "Query17dOptJoinTreeOptOrderingFixedHJOrdering", "Query17eOptJoinTreeOptOrderingFixedHJOrdering", "Query17fOptJoinTreeOptOrderingFixedHJOrdering", "Query18aOptJoinTreeOptOrderingFixedHJOrdering", "Query18bOptJoinTreeOptOrderingFixedHJOrdering", "Query18cOptJoinTreeOptOrderingFixedHJOrdering", "Query19aOptJoinTreeOptOrderingFixedHJOrdering", "Query19bOptJoinTreeOptOrderingFixedHJOrdering", "Query19cOptJoinTreeOptOrderingFixedHJOrdering", "Query19dOptJoinTreeOptOrderingFixedHJOrdering", "Query1aOptJoinTreeOptOrderingFixedHJOrdering", "Query1bOptJoinTreeOptOrderingFixedHJOrdering", "Query1cOptJoinTreeOptOrderingFixedHJOrdering", "Query1dOptJoinTreeOptOrderingFixedHJOrdering", "Query20aOptJoinTreeOptOrderingFixedHJOrdering", "Query20bOptJoinTreeOptOrderingFixedHJOrdering", "Query20cOptJoinTreeOptOrderingFixedHJOrdering", "Query21aOptJoinTreeOptOrderingFixedHJOrdering", "Query21bOptJoinTreeOptOrderingFixedHJOrdering", "Query21cOptJoinTreeOptOrderingFixedHJOrdering", "Query22aOptJoinTreeOptOrderingFixedHJOrdering", "Query22bOptJoinTreeOptOrderingFixedHJOrdering", "Query22cOptJoinTreeOptOrderingFixedHJOrdering", "Query22dOptJoinTreeOptOrderingFixedHJOrdering", "Query23aOptJoinTreeOptOrderingFixedHJOrdering", "Query23bOptJoinTreeOptOrderingFixedHJOrdering", "Query23cOptJoinTreeOptOrderingFixedHJOrdering", "Query24aOptJoinTreeOptOrderingFixedHJOrdering", "Query24bOptJoinTreeOptOrderingFixedHJOrdering", "Query25aOptJoinTreeOptOrderingFixedHJOrdering", "Query25bOptJoinTreeOptOrderingFixedHJOrdering", "Query25cOptJoinTreeOptOrderingFixedHJOrdering", "Query26aOptJoinTreeOptOrderingFixedHJOrdering", "Query26bOptJoinTreeOptOrderingFixedHJOrdering", "Query26cOptJoinTreeOptOrderingFixedHJOrdering", "Query27aOptJoinTreeOptOrderingFixedHJOrdering", "Query27bOptJoinTreeOptOrderingFixedHJOrdering", "Query27cOptJoinTreeOptOrderingFixedHJOrdering", "Query28aOptJoinTreeOptOrderingFixedHJOrdering", "Query28bOptJoinTreeOptOrderingFixedHJOrdering", "Query28cOptJoinTreeOptOrderingFixedHJOrdering", "Query29aOptJoinTreeOptOrderingFixedHJOrdering", "Query29bOptJoinTreeOptOrderingFixedHJOrdering", "Query29cOptJoinTreeOptOrderingFixedHJOrdering", "Query2aOptJoinTreeOptOrderingFixedHJOrdering", "Query2bOptJoinTreeOptOrderingFixedHJOrdering", "Query2cOptJoinTreeOptOrderingFixedHJOrdering", "Query2dOptJoinTreeOptOrderingFixedHJOrdering", "Query30aOptJoinTreeOptOrderingFixedHJOrdering", "Query30bOptJoinTreeOptOrderingFixedHJOrdering", "Query30cOptJoinTreeOptOrderingFixedHJOrdering", "Query31aOptJoinTreeOptOrderingFixedHJOrdering", "Query31bOptJoinTreeOptOrderingFixedHJOrdering", "Query31cOptJoinTreeOptOrderingFixedHJOrdering", "Query32aOptJoinTreeOptOrderingFixedHJOrdering", "Query32bOptJoinTreeOptOrderingFixedHJOrdering", "Query33aOptJoinTreeOptOrderingFixedHJOrdering", "Query33bOptJoinTreeOptOrderingFixedHJOrdering", "Query33cOptJoinTreeOptOrderingFixedHJOrdering", "Query3aOptJoinTreeOptOrderingFixedHJOrdering", "Query3bOptJoinTreeOptOrderingFixedHJOrdering", "Query3cOptJoinTreeOptOrderingFixedHJOrdering", "Query4aOptJoinTreeOptOrderingFixedHJOrdering", "Query4bOptJoinTreeOptOrderingFixedHJOrdering", "Query4cOptJoinTreeOptOrderingFixedHJOrdering", "Query5aOptJoinTreeOptOrderingFixedHJOrdering", "Query5bOptJoinTreeOptOrderingFixedHJOrdering", "Query5cOptJoinTreeOptOrderingFixedHJOrdering", "Query6aOptJoinTreeOptOrderingFixedHJOrdering", "Query6bOptJoinTreeOptOrderingFixedHJOrdering", "Query6cOptJoinTreeOptOrderingFixedHJOrdering", "Query6dOptJoinTreeOptOrderingFixedHJOrdering", "Query6eOptJoinTreeOptOrderingFixedHJOrdering", "Query6fOptJoinTreeOptOrderingFixedHJOrdering", "Query7aOptJoinTreeOptOrderingFixedHJOrdering", "Query7bOptJoinTreeOptOrderingFixedHJOrdering", "Query7cOptJoinTreeOptOrderingFixedHJOrdering", "Query8aOptJoinTreeOptOrderingFixedHJOrdering", "Query8bOptJoinTreeOptOrderingFixedHJOrdering", "Query8cOptJoinTreeOptOrderingFixedHJOrdering", "Query8dOptJoinTreeOptOrderingFixedHJOrdering", "Query9aOptJoinTreeOptOrderingFixedHJOrdering", "Query9bOptJoinTreeOptOrderingFixedHJOrdering", "Query9cOptJoinTreeOptOrderingFixedHJOrdering", "Query9dOptJoinTreeOptOrderingFixedHJOrdering"})
        public JOBQueriesFixedHJOrdering jobQueries;
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

    @Benchmark
    public void benchTTJFixedHJOrdering(BenchStateTTJFixedHJOrdering state)
    {
        state.query.eval();
    }

    @Benchmark
    public void benchHJ(BenchStateHJ state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkJOBWithPredicatesFixedHJOrdering.class,
                JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH).run();
    }
}
