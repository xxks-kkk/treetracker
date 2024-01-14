package org.zhu45.treetracker.benchmark.job.q19;

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
@Warmup(iterations = 0)
@Measurement(iterations = 1)
public class Q19FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q191TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query191.class);
    }

    @Benchmark
    public void q191HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query191.class);
    }

    @Benchmark
    public void q191LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query191.class);
    }

    @Benchmark
    public void q191Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query191.class);
    }

    @Benchmark
    public void q192TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query192.class);
    }

    @Benchmark
    public void q192HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query192.class);
    }

    @Benchmark
    public void q192LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query192.class);
    }

    @Benchmark
    public void q192Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query192.class);
    }

    @Benchmark
    public void q193TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query193.class);
    }

    @Benchmark
    public void q193HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query193.class);
    }

    @Benchmark
    public void q193LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query193.class);
    }

    @Benchmark
    public void q193Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query193.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q19FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
