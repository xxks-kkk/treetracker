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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 2)
public class BenchmarkTPCH
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @Param({"TTJHP", "HASH_JOIN", "LIP", "Yannakakis"})
        public JoinOperator joinOperator;
        @Param({"Q2", "Q3", "Q9", "Q10", "Q11", "Q12", "Q14", "Q15", "Q16", "Q17", "Q18", "Q19", "Q20", "Q21", "Q22"})
        public TPCHQueries tpchQueries;
        JoinFragmentType query;

        @Setup(Level.Iteration)
        public void setUp()
        {
            query = queryProvider(joinOperator, tpchQueries, duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Iteration)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @Benchmark
    public void bench(BenchState state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkTPCH.class, TPCH_RESULT_STORED_PATH).run();
    }
}
