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
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesFindOptJoinTree;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesYannakakis;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesYannakakis1Pass;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesYannakakisB;
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
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class BenchmarkHvTTPCHDifferentOrdering
{
    @State(Scope.Benchmark)
    public static class BenchStateHJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"HASH_JOIN"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WFindOptJoinTree", "Query11WFindOptJoinTree", "Query12WFindOptJoinTree", "Query14WFindOptJoinTree", "Query15WFindOptJoinTree", "Query16WFindOptJoinTree", "Query18WFindOptJoinTree", "Query19aWFindOptJoinTree", "Query19bWFindOptJoinTree", "Query19cWFindOptJoinTree", "Query20WFindOptJoinTree", "Query3WFindOptJoinTree", "Query7aWFindOptJoinTree", "Query7bWFindOptJoinTree", "Query8WFindOptJoinTree", "Query9WFindOptJoinTree"})
        public TPCHQueriesFindOptJoinTree tpchQueriesFindOptJoinTree;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    tpchQueriesFindOptJoinTree,
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
    public static class BenchStateTTJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
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
    public static class BenchStateYannakakis
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WOptJoinTreeOptOrderingY", "Query11WOptJoinTreeOptOrderingY", "Query12WOptJoinTreeOptOrderingY", "Query14WOptJoinTreeOptOrderingY", "Query15WOptJoinTreeOptOrderingY", "Query16WOptJoinTreeOptOrderingY", "Query18WOptJoinTreeOptOrderingY", "Query19aWOptJoinTreeOptOrderingY", "Query19bWOptJoinTreeOptOrderingY", "Query19cWOptJoinTreeOptOrderingY", "Query20WOptJoinTreeOptOrderingY", "Query3WOptJoinTreeOptOrderingY", "Query7aWOptJoinTreeOptOrderingY", "Query7bWOptJoinTreeOptOrderingY", "Query8WOptJoinTreeOptOrderingY", "Query9WOptJoinTreeOptOrderingY"})
        public TPCHQueriesYannakakis tpchQueriesYannakakis;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    tpchQueriesYannakakis,
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
        @Param({"Query10WOptJoinTreeOptOrderingYB", "Query11WOptJoinTreeOptOrderingYB", "Query12WOptJoinTreeOptOrderingYB", "Query14WOptJoinTreeOptOrderingYB", "Query15WOptJoinTreeOptOrderingYB", "Query16WOptJoinTreeOptOrderingYB", "Query18WOptJoinTreeOptOrderingYB", "Query19aWOptJoinTreeOptOrderingYB", "Query19bWOptJoinTreeOptOrderingYB", "Query19cWOptJoinTreeOptOrderingYB", "Query20WOptJoinTreeOptOrderingYB", "Query3WOptJoinTreeOptOrderingYB", "Query7aWOptJoinTreeOptOrderingYB", "Query7bWOptJoinTreeOptOrderingYB", "Query8WOptJoinTreeOptOrderingYB", "Query9WOptJoinTreeOptOrderingYB"})
        public TPCHQueriesYannakakisB tpchQueriesYannakakisB;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    tpchQueriesYannakakisB,
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
    public static class BenchStateYannakakis1Pass
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis1Pass"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WOptJoinTreeOptOrderingY1P", "Query11WOptJoinTreeOptOrderingY1P", "Query12WOptJoinTreeOptOrderingY1P", "Query14WOptJoinTreeOptOrderingY1P", "Query15WOptJoinTreeOptOrderingY1P", "Query16WOptJoinTreeOptOrderingY1P", "Query18WOptJoinTreeOptOrderingY1P", "Query19aWOptJoinTreeOptOrderingY1P", "Query19bWOptJoinTreeOptOrderingY1P", "Query19cWOptJoinTreeOptOrderingY1P", "Query20WOptJoinTreeOptOrderingY1P", "Query3WOptJoinTreeOptOrderingY1P", "Query7aWOptJoinTreeOptOrderingY1P", "Query7bWOptJoinTreeOptOrderingY1P", "Query8WOptJoinTreeOptOrderingY1P", "Query9WOptJoinTreeOptOrderingY1P"})
        public TPCHQueriesYannakakis1Pass tpchQueriesYannakakis1Pass;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    tpchQueriesYannakakis1Pass,
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
    public void benchTTJ(BenchStateTTJ state) throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchHJ(BenchStateHJ state) throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchYannakakis(BenchStateYannakakis state) throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchYannakakisB(BenchStateYannakakisB state) throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchYannakakis1Pass(BenchStateYannakakis1Pass state) throws InterruptedException
    {
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        Thread.sleep(2000);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkHvTTPCHDifferentOrdering.class,
                SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH).run();
    }
}
