package org.zhu45.treetracker.benchmark.yvt;

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
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

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
public class BenchmarkYvTJOB
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Yannakakis", "TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Q1a", "Q1b", "Q1c", "Q1d", "Q2a", "Q2b", "Q2c", "Q2d", "Q3a", "Q3b", "Q3c", "Q4a", "Q4b", "Q4c",
                "Q5a", "Q5b", "Q5c", "Q6a", "Q6b", "Q6c", "Q6d", "Q6e", "Q6f", "Q7a", "Q7b", "Q7c", "Q8a", "Q8b", "Q8c",
                "Q8d", "Q9a", "Q9b", "Q9c", "Q9d", "Q10a", "Q10b", "Q10c", "Q11a",
                "Q11b", "Q11c", "Q11d", "Q12a", "Q12b", "Q12c", "Q13a", "Q13b", "Q13c", "Q13d", "Q14a", "Q14b",
                "Q14c", "Q15a", "Q15b", "Q15c", "Q15d", "Q16a", "Q16b", "Q16c", "Q16d", "Q17a", "Q17b", "Q17c", "Q17d",
                "Q17e", "Q17f", "Q18a", "Q18b", "Q18c", "Q19a", "Q19b", "Q19c", "Q19d", "Q20a", "Q20b", "Q20c", "Q21a", "Q21b", "Q21c",
                "Q22a", "Q22b", "Q22c", "Q22d", "Q23a", "Q23b", "Q23c", "Q24a", "Q24b", "Q25a", "Q25b", "Q25c", "Q26a", "Q26b", "Q26c",
                "Q27a", "Q27b", "Q27c", "Q28a", "Q28b", "Q28c", "Q29a", "Q29b", "Q29c", "Q30a", "Q30b", "Q30c", "Q31a", "Q31b", "Q31c",
                "Q32a", "Q32b", "Q33a", "Q33b", "Q33c"})
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
        createStatisticsJson(state.query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkYvTJOB.class, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH).run();
    }
}
