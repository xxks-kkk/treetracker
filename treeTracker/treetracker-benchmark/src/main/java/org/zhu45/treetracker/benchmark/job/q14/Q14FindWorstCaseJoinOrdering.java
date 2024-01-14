package org.zhu45.treetracker.benchmark.job.q14;

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
public class Q14FindWorstCaseJoinOrdering
{
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
    public void q141TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query141.class);
    }

    @Benchmark
    public void q141HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query141.class);
    }

    @Benchmark
    public void q141LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query141.class);
    }

    @Benchmark
    public void q141Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query141.class);
    }

    @Benchmark
    public void q142TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query142.class);
    }

    @Benchmark
    public void q142HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query142.class);
    }

    @Benchmark
    public void q142LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query142.class);
    }

    @Benchmark
    public void q142Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query142.class);
    }

    @Benchmark
    public void q143TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query143.class);
    }

    @Benchmark
    public void q143HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query143.class);
    }

    @Benchmark
    public void q143LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query143.class);
    }

    @Benchmark
    public void q143Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query143.class);
    }

    @Benchmark
    public void q144TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query144.class);
    }

    @Benchmark
    public void q144HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query144.class);
    }

    @Benchmark
    public void q144LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query144.class);
    }

    @Benchmark
    public void q144Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query144.class);
    }

    @Benchmark
    public void q145TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query145.class);
    }

    @Benchmark
    public void q145HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query145.class);
    }

    @Benchmark
    public void q145LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query145.class);
    }

    @Benchmark
    public void q145Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query145.class);
    }

    @Benchmark
    public void q146TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query146.class);
    }

    @Benchmark
    public void q146HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query146.class);
    }

    @Benchmark
    public void q146LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query146.class);
    }

    @Benchmark
    public void q146Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query146.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q14FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
