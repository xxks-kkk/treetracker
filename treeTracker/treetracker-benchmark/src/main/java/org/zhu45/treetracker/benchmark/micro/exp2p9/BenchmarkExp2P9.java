package org.zhu45.treetracker.benchmark.micro.exp2p9;

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
import static org.zhu45.treetracker.benchmark.Benchmarks.EXP2P9_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 10, warmups = 5, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
//@Fork(value = 1, warmups = 0, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
//@Warmup(iterations = 0, time = 2)
//@Measurement(iterations = 1, time = 2)
public class BenchmarkExp2P9
{
    @State(Scope.Benchmark)
    public static class BenchStateTTJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP_BF", "TTJHP_NO_NG"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Exp2P9Query0P", "Exp2P9Query1P", "Exp2P9Query2P", "Exp2P9Query3P", "Exp2P9Query4P", "Exp2P9Query5P", "Exp2P9Query6P",
                "Exp2P9Query7P", "Exp2P9Query8P", "Exp2P9Query9P", "Exp2P9Query10P", "Exp2P9Query20P", "Exp2P9Query30P", "Exp2P9Query40P",
                "Exp2P9Query50P", "Exp2P9Query60P", "Exp2P9Query70P", "Exp2P9Query80P", "Exp2P9Query90P",
                "Exp2P9Query100P"})
        public Exp2P9Queries exp2P9Queries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator, exp2P9Queries,
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
        benchmark(BenchmarkExp2P9.class,
                EXP2P9_RESULT_STORED_PATH).run();
    }
}
