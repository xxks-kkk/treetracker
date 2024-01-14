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
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.benchmark.backjump.BackJumpQuery;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.BACKJUMP_OTHERS_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@State(Scope.Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 1)
@Measurement(iterations = 3)
public class BenchmarkYvTBackJump
{
    @State(Scope.Benchmark)
    public static class StudyOfTTJHPandYannakakis
    {
        @Param({"TTJHP", "Yannakakis"})
        public JoinOperator joinOperator;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"2"})
        public int numberOfBackJumpedRelations;
        @Param({"256", "1024", "4096", "16384", "65536"})
        public long backJumpedRelationSize;
        JoinFragmentType query;

        // TODO: probably need to set to Level.Iteration and re-run
        @Setup(Level.Invocation)
        public void setUp()
        {
            QueryProviderContext context = QueryProviderContext.builder()
                    .setJoinOperator(joinOperator)
                    .setQueryClazz(BackJumpQuery.class)
                    .setBackJumpedRelationSize(backJumpedRelationSize)
                    .setNumberOfBackjumpedRelations(numberOfBackJumpedRelations)
                    .setQueryName(String.format("BackJumpQuery_%s_%s", backJumpedRelationSize, numberOfBackJumpedRelations))
                    .setJdbcClient(duckDBJdbcClientSupplier.get())
                    .build();
            query = queryProvider(context);
        }

        // TODO: probably need to set to Level.Iteration and re-run
        @TearDown(Level.Invocation)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class StudyOfTTJHPandYannakakis2
    {
        @Param({"TTJHP", "Yannakakis"})
        public JoinOperator joinOperator;
        @Param({"1", "2", "4", "8", "16", "32"})
        public int numberOfBackJumpedRelations;
        @SuppressWarnings("checkstyle:AnnotationUseStyle")
        @Param({"2"})
        public long backJumpedRelationSize;
        JoinFragmentType query;

        // TODO: probably need to set to Level.Iteration and re-run
        @Setup(Level.Invocation)
        public void setUp()
        {
            QueryProviderContext context = QueryProviderContext.builder()
                    .setJoinOperator(joinOperator)
                    .setQueryClazz(BackJumpQuery.class)
                    .setBackJumpedRelationSize(backJumpedRelationSize)
                    .setNumberOfBackjumpedRelations(numberOfBackJumpedRelations)
                    .setQueryName(String.format("BackJumpQuery_%s_%s", backJumpedRelationSize, numberOfBackJumpedRelations))
                    .setJdbcClient(duckDBJdbcClientSupplier.get())
                    .build();
            query = queryProvider(context);
        }

        // TODO: probably need to set to Level.Iteration and re-run
        @TearDown(Level.Invocation)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @Benchmark
    public void bench(StudyOfTTJHPandYannakakis state)
    {
        createStatisticsJson(state.query, BACKJUMP_OTHERS_RESULT_STORED_PATH);
    }

    @Benchmark
    public void bench2(StudyOfTTJHPandYannakakis2 state)
    {
        createStatisticsJson(state.query, BACKJUMP_OTHERS_RESULT_STORED_PATH);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkYvTBackJump.class, BACKJUMP_OTHERS_RESULT_STORED_PATH).run();
    }
}
