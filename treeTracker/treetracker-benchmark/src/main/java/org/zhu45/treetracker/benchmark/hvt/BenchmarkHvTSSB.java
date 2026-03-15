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
import org.zhu45.treetracker.benchmark.ssb.SSBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH;
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
public class BenchmarkHvTSSB
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis1Pass", "TTJHP", "HASH_JOIN", "TTJHP_NO_NG"})
        public JoinOperator joinOperator;
        @Param({"Q1P1", "Q1P2", "Q1P3", "Q2P1", "Q2P2", "Q2P3", "Q3P1", "Q3P2", "Q3P3", "Q3P4", "Q4P1", "Q4P2", "Q4P3"})
        public SSBQueries ssbQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator, ssbQueries, duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @Benchmark
    public void bench(BenchState state)
            throws InterruptedException
    {
        createStatisticsJson(state.query, SSB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH);
        Thread.sleep(2000);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkHvTSSB.class, SSB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH).run();
    }
}
