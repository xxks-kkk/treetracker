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
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 2, warmups = 2, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class BenchmarkJOBWithPredicates
{
    @State(Scope.Benchmark)
    public static class BenchState
    {
        //        @Param({"Yannakakis", "TTJHP", "HASH_JOIN", "LIP"})
        @Param({"TTJHP", "HASH_JOIN"})
        public JoinOperator joinOperator;
        //    @Param({"Q1b", "Q1c", "Q1d", "Q2b", "Q2c", "Q2d", "Q3b", "Q3c", "Q4b", "Q4c", "Q5b", "Q5c", "Q6b",
//                "Q6c", "Q6d", "Q6e", "Q6f", "Q7b", "Q7c", "Q8b", "Q8c", "Q8d", "Q9b", "Q9c", "Q9d", "Q10b", "Q10c",
//                "Q11b", "Q11c", "Q11d", "Q12b", "Q12c", "Q13b", "Q13c", "Q13d", "Q14a", "Q14b",
//                "Q14c", "Q15b", "Q15c", "Q15d", "Q16b", "Q16c", "Q16d", "Q17b", "Q17c", "Q17d",
//                "Q17e", "Q17f", "Q18b", "Q18c", "Q19b", "Q19c", "Q19d", "Q20b", "Q20c", "Q21b", "Q21c",
//                "Q22b", "Q22c", "Q22d", "Q23b", "Q23c", "Q24a", "Q24b", "Q25b", "Q25c", "Q26b", "Q26c",
//               "Q27b", "Q27c", "Q28b", "Q28c", "Q29b", "Q29c", "Q30b", "Q30c", "Q31b", "Q31c", "Q32b", "Q33b", "Q33c"})
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Q1aOptJoinTreeOptOrdering", "Q1bOptJoinTreeOptOrdering", "Q1cOptJoinTreeOptOrdering", "Q1dOptJoinTreeOptOrdering",
                "Q2aOptJoinTreeOptOrdering", "Q2bOptJoinTreeOptOrdering", "Q2cOptJoinTreeOptOrdering", "Q2dOptJoinTreeOptOrdering",
                "Q3aOptJoinTreeOptOrdering", "Q3bOptJoinTreeOptOrdering", "Q3cOptJoinTreeOptOrdering",
                "Q4aOptJoinTreeOptOrdering", "Q4bOptJoinTreeOptOrdering", "Q4cOptJoinTreeOptOrdering",
                "Q5aOptJoinTreeOptOrdering", "Q5bOptJoinTreeOptOrdering", "Q5cOptJoinTreeOptOrdering",
                "Q6aOptJoinTreeOptOrdering", "Q6bOptJoinTreeOptOrdering", "Q6cOptJoinTreeOptOrdering", "Q6dOptJoinTreeOptOrdering", "Q6eOptJoinTreeOptOrdering",
                "Q7aOptJoinTreeOptOrdering", "Q8cOptJoinTreeOptOrdering"})
        public JOBQueries jobQueries;
        JoinFragmentType query;

        @Setup(Level.Invocation)
        public void setUp()
        {
            query = queryProvider(joinOperator, jobQueries, duckDBJdbcClientSupplier.get());
            // query.populateDomain();
        }

        @TearDown(Level.Invocation)
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
        benchmark(BenchmarkJOBWithPredicates.class, JOB_WITH_PREDICATES_RESULT_STORED_PATH).run();
    }
}
