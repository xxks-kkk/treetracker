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
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesAutoGen;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesHashJoinOnYannakakis;
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

/**
 * Collect HJ statistics based on other algorithms' orderings. This is for statistics
 * analysis.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class BenchmarkHvTTPCHHJOnOtherOrdering
{
    @State(Scope.Benchmark)
    public static class BenchStateHJonTTJOrder
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"HASH_JOIN"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WOptJoinTreeOptOrdering", "Query11WOptJoinTreeOptOrdering", "Query12WOptJoinTreeOptOrdering", "Query14WOptJoinTreeOptOrdering", "Query15WOptJoinTreeOptOrdering", "Query16WOptJoinTreeOptOrdering", "Query18WOptJoinTreeOptOrdering", "Query19aWOptJoinTreeOptOrdering", "Query19bWOptJoinTreeOptOrdering", "Query19cWOptJoinTreeOptOrdering", "Query20WOptJoinTreeOptOrdering", "Query3WOptJoinTreeOptOrdering", "Query7aWOptJoinTreeOptOrdering", "Query7bWOptJoinTreeOptOrdering", "Query8WOptJoinTreeOptOrdering", "Query9WOptJoinTreeOptOrdering"})
        public TPCHQueriesAutoGen tpchQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator, tpchQueries,
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
    public static class BenchStateHJonYannakakisOrder
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"HASH_JOIN"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WOptJoinTreeOptOrderingHJOnY", "Query11WOptJoinTreeOptOrderingHJOnY", "Query12WOptJoinTreeOptOrderingHJOnY",
                "Query14WOptJoinTreeOptOrderingHJOnY", "Query15WOptJoinTreeOptOrderingHJOnY", "Query16WOptJoinTreeOptOrderingHJOnY",
                "Query18WOptJoinTreeOptOrderingHJOnY", "Query19aWOptJoinTreeOptOrderingHJOnY", "Query19bWOptJoinTreeOptOrderingHJOnY",
                "Query19cWOptJoinTreeOptOrderingHJOnY", "Query20WOptJoinTreeOptOrderingHJOnY", "Query3WOptJoinTreeOptOrderingHJOnY",
                "Query7aWOptJoinTreeOptOrderingHJOnY", "Query7bWOptJoinTreeOptOrderingHJOnY", "Query8WOptJoinTreeOptOrderingHJOnY",
                "Query9WOptJoinTreeOptOrderingHJOnY"})
        public TPCHQueriesHashJoinOnYannakakis tpchQueriesHashJoinOnYannakakis;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    tpchQueriesHashJoinOnYannakakis,
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
    public void benchTTJ(BenchStateHJonTTJOrder state) throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchYannakakis(BenchStateHJonYannakakisOrder state) throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkHvTTPCHHJOnOtherOrdering.class,
                SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH).run();
    }
}
