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
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesFindOptJoinTree;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesShallowHJOrdering;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesYannakakis1PassShallowHJOrdering;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH;
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
public class BenchmarkHvTTPCHHJOrderingShallow
{
    @State(Scope.Benchmark)
    public static class BenchStateTTJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WOptJoinTreeOptOrderingShallowHJOrdering", "Query11WOptJoinTreeOptOrderingShallowHJOrdering", "Query12WOptJoinTreeOptOrderingShallowHJOrdering",
                "Query14WOptJoinTreeOptOrderingShallowHJOrdering", "Query15WOptJoinTreeOptOrderingShallowHJOrdering", "Query16WOptJoinTreeOptOrderingShallowHJOrdering",
                "Query18WOptJoinTreeOptOrderingShallowHJOrdering", "Query19aWOptJoinTreeOptOrderingShallowHJOrdering", "Query19bWOptJoinTreeOptOrderingShallowHJOrdering",
                "Query19cWOptJoinTreeOptOrderingShallowHJOrdering", "Query20WOptJoinTreeOptOrderingShallowHJOrdering", "Query3WOptJoinTreeOptOrderingShallowHJOrdering",
                "Query7aWOptJoinTreeOptOrderingShallowHJOrdering", "Query7bWOptJoinTreeOptOrderingShallowHJOrdering", "Query8WOptJoinTreeOptOrderingShallowHJOrdering",
                "Query9WOptJoinTreeOptOrderingShallowHJOrdering"})
        public TPCHQueriesShallowHJOrdering tpchQueries;
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
    public static class BenchStateHJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"HASH_JOIN"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WFindOptJoinTree", "Query11WFindOptJoinTree", "Query12WFindOptJoinTree", "Query14WFindOptJoinTree", "Query15WFindOptJoinTree", "Query16WFindOptJoinTree",
                "Query18WFindOptJoinTree", "Query19aWFindOptJoinTree", "Query19bWFindOptJoinTree", "Query19cWFindOptJoinTree", "Query20WFindOptJoinTree", "Query3WFindOptJoinTree",
                "Query7aWFindOptJoinTree", "Query7bWFindOptJoinTree", "Query8WFindOptJoinTree", "Query9WFindOptJoinTree"})
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
    public static class BenchStateYannakakis1Pass
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis1Pass"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query11WOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query12WOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query14WOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query15WOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query16WOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query18WOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query19aWOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query19bWOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query19cWOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query20WOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query3WOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query7aWOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query7bWOptJoinTreeOptOrderingY1PShallowHJOrdering", "Query8WOptJoinTreeOptOrderingY1PShallowHJOrdering",
                "Query9WOptJoinTreeOptOrderingY1PShallowHJOrdering"})
        public TPCHQueriesYannakakis1PassShallowHJOrdering tpchQueriesYannakakis1Pass;
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
    public void benchTTJ(BenchStateTTJ state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, TPCH_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchHJ(BenchStateHJ state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, TPCH_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH);
        Thread.sleep(2000);
    }

    @Benchmark
    public void benchYA(BenchStateYannakakis1Pass state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, TPCH_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH);
        Thread.sleep(2000);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkHvTTPCHHJOrderingShallow.class, TPCH_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH).run();
    }
}
