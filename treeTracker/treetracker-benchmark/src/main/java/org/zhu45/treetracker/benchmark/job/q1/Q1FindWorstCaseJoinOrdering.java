package org.zhu45.treetracker.benchmark.job.q1;

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
public class Q1FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q11TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query11.class);
    }

    @Benchmark
    public void q11HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query11.class);
    }

    @Benchmark
    public void q11LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query11.class);
    }

    @Benchmark
    public void q11Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query11.class);
    }

    @Benchmark
    public void q12TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query12.class);
    }

    @Benchmark
    public void q12HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query12.class);
    }

    @Benchmark
    public void q12LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query12.class);
    }

    @Benchmark
    public void q12Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query12.class);
    }

    @Benchmark
    public void q13TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query13.class);
    }

    @Benchmark
    public void q13HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query13.class);
    }

    @Benchmark
    public void q13LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query13.class);
    }

    @Benchmark
    public void q13Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query13.class);
    }

    @Benchmark
    public void q14TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query14.class);
    }

    @Benchmark
    public void q14HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query14.class);
    }

    @Benchmark
    public void q14LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query14.class);
    }

    @Benchmark
    public void q14Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query14.class);
    }

    @Benchmark
    public void q15TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query15.class);
    }

    @Benchmark
    public void q15HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query15.class);
    }

    @Benchmark
    public void q15LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query15.class);
    }

    @Benchmark
    public void q15Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query15.class);
    }

    @Benchmark
    public void q16TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query16.class);
    }

    @Benchmark
    public void q16HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query16.class);
    }

    @Benchmark
    public void q16LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query16.class);
    }

    @Benchmark
    public void q16Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query16.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q1FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
