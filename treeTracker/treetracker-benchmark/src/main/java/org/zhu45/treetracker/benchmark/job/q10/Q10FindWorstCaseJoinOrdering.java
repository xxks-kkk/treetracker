package org.zhu45.treetracker.benchmark.job.q10;

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
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.benchmarkQuery;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 1)
@Measurement(iterations = 1)
public class Q10FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q101TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query101.class);
    }

    @Benchmark
    public void q101HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query101.class);
    }

    @Benchmark
    public void q101LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query101.class);
    }

    @Benchmark
    public void q101Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query101.class);
    }

    @Benchmark
    public void q102TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query102.class);
    }

    @Benchmark
    public void q102HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query102.class);
    }

    @Benchmark
    public void q102LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query102.class);
    }

    @Benchmark
    public void q102Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query102.class);
    }

    @Benchmark
    public void q103TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query103.class);
    }

    @Benchmark
    public void q103HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query103.class);
    }

    @Benchmark
    public void q103LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query103.class);
    }

    @Benchmark
    public void q103Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query103.class);
    }

    @Benchmark
    public void q104TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query104.class);
    }

    @Benchmark
    public void q104HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query104.class);
    }

    @Benchmark
    public void q104LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query104.class);
    }

    @Benchmark
    public void q104Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query104.class);
    }

    @Benchmark
    public void q105TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query105.class);
    }

    @Benchmark
    public void q105HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query105.class);
    }

    @Benchmark
    public void q105LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query105.class);
    }

    @Benchmark
    public void q105Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query105.class);
    }

    @Benchmark
    public void q106TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query106.class);
    }

    @Benchmark
    public void q106HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query106.class);
    }

    @Benchmark
    public void q106LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query106.class);
    }

    @Benchmark
    public void q106Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query106.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q10FindWorstCaseJoinOrdering.class).run();
    }
}
