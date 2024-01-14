package org.zhu45.treetracker.benchmark.micro.exp2p8;

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
import static org.zhu45.treetracker.benchmark.Benchmarks.EXP2P8_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 10, warmups = 5, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BenchmarkExp2P8
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP_BF"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Exp2P8JoinTree1QueryK1N10", "Exp2P8JoinTree2QueryK1N10", "Exp2P8JoinTree1QueryK2N10", "Exp2P8JoinTree2QueryK2N10", "Exp2P8JoinTree1QueryK3N10", "Exp2P8JoinTree2QueryK3N10", "Exp2P8JoinTree1QueryK4N10", "Exp2P8JoinTree2QueryK4N10", "Exp2P8JoinTree1QueryK5N10", "Exp2P8JoinTree2QueryK5N10", "Exp2P8JoinTree1QueryK6N10", "Exp2P8JoinTree2QueryK6N10", "Exp2P8JoinTree1QueryK7N10", "Exp2P8JoinTree2QueryK7N10", "Exp2P8JoinTree1QueryK8N10", "Exp2P8JoinTree2QueryK8N10"})
        public Exp2P8Queries exp2p8Queries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator, exp2p8Queries,
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
    public void bench(BenchState state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkExp2P8.class, EXP2P8_RESULT_STORED_PATH).run();
    }
}
