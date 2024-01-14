package org.zhu45.treetracker.benchmark.job;

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
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class BenchmarkJOB
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis", "TTJHP", "HASH_JOIN", "LIP"})
        public JoinOperator joinOperator;
        @Param({"Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9", "Q10", "Q11", "Q12", "Q13", "Q14", "Q15",
                "Q16", "Q17", "Q18", "Q19", "Q20", "Q21", "Q22", "Q23", "Q25", "Q26", "Q27", "Q28", "Q30", "Q31", "Q32", "Q33"})
        public JOBQueries jobQueries;
        JoinFragmentType query;

        @Setup(Level.Iteration)
        public void setUp()
        {
            query = queryProvider(joinOperator, jobQueries, duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Iteration)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class JOBQ31
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis", "TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Q31"})
        public JOBQueries jobQueries;
        JoinFragmentType query;

        @Setup(Level.Iteration)
        public void setUp()
        {
            query = queryProvider(joinOperator, jobQueries, duckDBJdbcClientSupplier.get());
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

    @Benchmark
    public void benchJOBQ31(JOBQ31 state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkJOB.class, JOB_RESULT_STORED_PATH).run();
    }
}
