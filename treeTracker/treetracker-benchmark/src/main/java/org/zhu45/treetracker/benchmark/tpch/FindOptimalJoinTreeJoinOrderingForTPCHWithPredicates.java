package org.zhu45.treetracker.benchmark.tpch;

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
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstProvider;
import org.zhu45.treetracker.relational.planner.cost.TTJCardEstProvider;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingJoinTreeWithDP;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProviderConfiguration.defaultConfiguration;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class FindOptimalJoinTreeJoinOrderingForTPCHWithPredicates
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query10WFindOptJoinTree", "Query11WFindOptJoinTree", "Query12WFindOptJoinTree", "Query14WFindOptJoinTree", "Query15WFindOptJoinTree", "Query16WFindOptJoinTree",
                "Query18WFindOptJoinTree", "Query19aWFindOptJoinTree", "Query19bWFindOptJoinTree", "Query19cWFindOptJoinTree", "Query20WFindOptJoinTree", "Query3WFindOptJoinTree",
                "Query7aWFindOptJoinTree", "Query7bWFindOptJoinTree", "Query8WFindOptJoinTree", "Query9WFindOptJoinTree"})
        public TPCHQueriesFindOptJoinTree jobQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    jobQueries,
                    List.of(new FindTheBestJoinOrderingJoinTreeWithDP(new TTJCardEstProvider(new JoinTreeCostEstProvider(defaultConfiguration)),
                            FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration.builder().enableHJPhysicalHeuristics(true).build())),
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
    {
        state.query.getPlan().getPlanStatistics().save(state.query.getQueryName(),
                state.query.getAlgorithm(), TPCH_WITH_PREDICATES_RESULT_STORED_PATH);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(FindOptimalJoinTreeJoinOrderingForTPCHWithPredicates.class, TPCH_WITH_PREDICATES_RESULT_STORED_PATH).run();
    }
}
