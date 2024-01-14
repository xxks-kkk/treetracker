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
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_EXP2P3_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

/**
 * Exp 2.3: Effectiveness of Algorithmic Features in TTJ on JOB
 */
@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 10, warmups = 5, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class Exp2P3JOB
{
    @State(Scope.Benchmark)
    public static class BenchStateTTJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP_BG"})
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

    @Benchmark
    public void benchTTJ(BenchStateTTJ state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Exp2P3JOB.class,
                JOB_WITH_PREDICATES_RESULT_EXP2P3_STORED_PATH).run();
    }
}
