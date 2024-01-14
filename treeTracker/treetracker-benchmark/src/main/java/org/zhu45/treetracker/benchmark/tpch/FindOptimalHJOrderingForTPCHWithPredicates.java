package org.zhu45.treetracker.benchmark.tpch;

import org.json.simple.parser.ParseException;
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
import org.zhu45.treetracker.relational.planner.cost.PostgresCardEstProvider;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingJoinTreeWithDP;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP;

import java.io.IOException;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.benchmark.statsgen.GenerateOptimalJoinOrderingCSV.generateOptimalJoinOrderingCSV;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.CodeGen.relation2MultiwayJoinNode;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class FindOptimalHJOrderingForTPCHWithPredicates
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"HASH_JOIN"})
        public JoinOperator joinOperator;
        @Param({"Q3W", "Q7aW", "Q7bW", "Q8W", "Q9W", "Q10W", "Q11W", "Q12W", "Q14W", "Q15W", "Q16W",
                "Q18W", "Q19aW", "Q19bW", "Q19cW", "Q20W"})
        public TPCHQueries jobQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    jobQueries,
                    List.of(new FindTheBestJoinOrderingWithDP(new PostgresCardEstProvider(),
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
            throws RunnerException, IOException, ParseException
    {
        benchmark(FindOptimalHJOrderingForTPCHWithPredicates.class, TPCH_WITH_PREDICATES_RESULT_STORED_PATH).run();
        generateOptimalJoinOrderingCSV(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, relation2MultiwayJoinNode);
    }
}
