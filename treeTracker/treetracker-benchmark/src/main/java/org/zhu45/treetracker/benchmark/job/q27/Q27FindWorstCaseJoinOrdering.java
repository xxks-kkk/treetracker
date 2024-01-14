package org.zhu45.treetracker.benchmark.job.q27;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.zhu45.treetracker.benchmark.Benchmarks.OTHERS_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.benchmarkQuery;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class Q27FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q271TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query271.class);
    }

    @Benchmark
    public void q271HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query271.class);
    }

    @Benchmark
    public void q271LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query271.class);
    }

    @Benchmark
    public void q271Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query271.class);
    }

    @Benchmark
    public void q272TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query272.class);
    }

    @Benchmark
    public void q272HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query272.class);
    }

    @Benchmark
    public void q272LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query272.class);
    }

    @Benchmark
    public void q272Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query272.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q27FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
