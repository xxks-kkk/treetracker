package org.zhu45.treetracker.benchmark.ssb;

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
import org.zhu45.treetracker.relational.planner.cost.JoinTreeHeightProvider;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeHeightYannakakisProvider;
import org.zhu45.treetracker.relational.planner.rule.DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee;
import org.zhu45.treetracker.relational.planner.rule.FindOptimalJoinTree;
import org.zhu45.treetracker.relational.planner.rule.RuleType;

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_SQLITE_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

/**
 * We use HJ ordering and generate the join tree with the smallest height.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class FindShallowJoinTreeForSSB
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query1P1FindOptJoinTree", "Query1P2FindOptJoinTree", "Query1P3FindOptJoinTree", "Query2P1FindOptJoinTree", "Query2P2FindOptJoinTree", "Query2P3FindOptJoinTree", "Query3P1FindOptJoinTree", "Query3P2FindOptJoinTree", "Query3P3FindOptJoinTree", "Query3P4FindOptJoinTree", "Query4P1FindOptJoinTree", "Query4P2FindOptJoinTree", "Query4P3FindOptJoinTree"})
        public SSBQueriesFindOptJoinTree ssbQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    ssbQueries,
                    List.of(new FindOptimalJoinTree(new JoinTreeHeightProvider()),
                            new DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee(RuleType.AS_A_WHOLE)),
                    duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchYannakakisState
    {
        // NOTE: we don't specify Yannakakis here because we will automatically invoke AttachFullReducer rule,
        // which causes exception because there is no MultiwayJoinNodde associated with each relation.
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query1P1FindOptJoinTree", "Query1P2FindOptJoinTree", "Query1P3FindOptJoinTree", "Query2P1FindOptJoinTree", "Query2P2FindOptJoinTree", "Query2P3FindOptJoinTree", "Query3P1FindOptJoinTree", "Query3P2FindOptJoinTree", "Query3P3FindOptJoinTree", "Query3P4FindOptJoinTree", "Query4P1FindOptJoinTree", "Query4P2FindOptJoinTree", "Query4P3FindOptJoinTree"})
        public SSBQueriesFindOptJoinTree ssbQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    ssbQueries,
                    List.of(new FindOptimalJoinTree(new JoinTreeHeightYannakakisProvider())),
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
                state.query.getAlgorithm(), SSB_SQLITE_ORDERING_STORED_PATH);
    }

    @Benchmark
    public void benchYannakakis(BenchYannakakisState state)
    {
        state.query.getPlan().getPlanStatistics().save(state.query.getQueryName(),
                JoinOperator.Yannakakis, SSB_SQLITE_ORDERING_STORED_PATH);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(FindShallowJoinTreeForSSB.class, SSB_SQLITE_ORDERING_STORED_PATH).run();
    }
}
