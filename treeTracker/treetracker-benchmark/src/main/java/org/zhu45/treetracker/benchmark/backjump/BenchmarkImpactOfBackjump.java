package org.zhu45.treetracker.benchmark.backjump;

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
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(value = 1, jvmArgsAppend = {"-Xmx30G", "-Xms10G"})
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class BenchmarkImpactOfBackjump
{
    @State(Scope.Benchmark)
    public static class ScaleWithBackJumpedRelationSize
    {
        @Param({"HASH_JOIN", "TTJHP", "LIP"})
        public JoinOperator joinOperator;
        @Param({"256", "1024", "4096", "16384", "65536"})
        public long backJumpedRelationSize;
        JoinFragmentType query;

        @Setup(Level.Iteration)
        public void setUp()
        {
            QueryProviderContext context = QueryProviderContext.builder()
                    .setJoinOperator(joinOperator)
                    .setQueryClazz(BackJumpQuery.class)
                    .setBackJumpedRelationSize(backJumpedRelationSize)
                    .setNumberOfBackjumpedRelations(2)
                    .setJdbcClient(postgresJdbcClientSupplier.get())
                    .build();
            query = queryProvider(context);
        }

        @TearDown(Level.Iteration)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class ScaleWithNumberOfBackJumpRelations
    {
        @Param({"TTJHP", "HASH_JOIN", "LIP"})
        public JoinOperator joinOperator;
        @Param({"1", "2", "4", "8", "16", "32"})
        public int numberOfBackJumpedRelations;
        JoinFragmentType query;

        @Setup(Level.Iteration)
        public void setUp()
        {
            QueryProviderContext context = QueryProviderContext.builder()
                    .setJoinOperator(joinOperator)
                    .setQueryClazz(BackJumpQuery.class)
                    .setBackJumpedRelationSize(2)
                    .setNumberOfBackjumpedRelations(numberOfBackJumpedRelations)
                    .build();
            query = queryProvider(context);
        }

        @TearDown(Level.Iteration)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @State(Scope.Benchmark)
    public static class StudyOfTTJHPandYannakakis
    {
        @Param({"TTJHP", "Yannakakis"})
        public JoinOperator joinOperator;
        @Param({"1", "2", "4", "8", "16", "32", "64", "128", "256"})
        public int numberOfBackJumpedRelations;
        @Param({"4", "8", "32", "64", "128", "256", "1024", "4096"})
        public long backJumpedRelationSize;
        JoinFragmentType query;

        @Setup(Level.Invocation)
        public void setUp()
        {
            QueryProviderContext context = QueryProviderContext.builder()
                    .setJoinOperator(joinOperator)
                    .setQueryClazz(BackJumpQuery.class)
                    .setBackJumpedRelationSize(backJumpedRelationSize)
                    .setNumberOfBackjumpedRelations(numberOfBackJumpedRelations)
                    .build();
            query = queryProvider(context);
        }

        @TearDown(Level.Invocation)
        public void tearDown()
        {
            query.cleanUp();
        }
    }

    @Benchmark
    public void benchScaleWithBackJumpedRelationSize(ScaleWithBackJumpedRelationSize state)
    {
        state.query.eval();
    }

    @Benchmark
    public void benchScaleWithNumberOfBackJumpRelations(ScaleWithNumberOfBackJumpRelations state)
    {
        state.query.eval();
    }

    @Benchmark
    public void benchStudyOfTTJHPandYannakakis(StudyOfTTJHPandYannakakis state)
    {
        state.query.eval();
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkImpactOfBackjump.class, JOB_RESULT_STORED_PATH).run();
    }
}
