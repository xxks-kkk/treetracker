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

import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 10, warmups = 5, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
public class BenchmarkSSBWithPredicatesDifferentOrdering
{
    @State(Scope.Benchmark)
    public static class BenchStateTTJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"TTJHP"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query1P1OptJoinTreeOptOrdering", "Query1P2OptJoinTreeOptOrdering", "Query1P3OptJoinTreeOptOrdering", "Query2P1OptJoinTreeOptOrdering", "Query2P2OptJoinTreeOptOrdering", "Query2P3OptJoinTreeOptOrdering", "Query3P1OptJoinTreeOptOrdering", "Query3P2OptJoinTreeOptOrdering", "Query3P3OptJoinTreeOptOrdering", "Query3P4OptJoinTreeOptOrdering", "Query4P1OptJoinTreeOptOrdering", "Query4P2OptJoinTreeOptOrdering", "Query4P3OptJoinTreeOptOrdering"})
        public SSBQueriesAutoGen ssbQueries;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator, ssbQueries,
                    List.of(),
                    duckDBJdbcClientSupplier.get());
        }

        @TearDown(Level.Trial)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class BenchStateHJ
    {
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"HASH_JOIN"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"Query1P1FindOptJoinTree", "Query1P2FindOptJoinTree", "Query1P3FindOptJoinTree", "Query2P1FindOptJoinTree", "Query2P2FindOptJoinTree", "Query2P3FindOptJoinTree", "Query3P1FindOptJoinTree", "Query3P2FindOptJoinTree", "Query3P3FindOptJoinTree", "Query3P4FindOptJoinTree", "Query4P1FindOptJoinTree", "Query4P2FindOptJoinTree", "Query4P3FindOptJoinTree"})
        public SSBQueriesFindOptJoinTree ssbQueriesFindOptJoinTree;
        JoinFragmentType query;

        @Setup(Level.Trial)
        public void setUp()
        {
            query = queryProvider(joinOperator,
                    ssbQueriesFindOptJoinTree,
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

    @Benchmark
    public void benchHJ(BenchStateHJ state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkSSBWithPredicatesDifferentOrdering.class,
                SSB_RESULT_STORED_PATH).run();
    }
}
