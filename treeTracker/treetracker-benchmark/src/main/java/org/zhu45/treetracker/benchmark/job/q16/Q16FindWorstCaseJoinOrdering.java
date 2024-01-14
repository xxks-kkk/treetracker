package org.zhu45.treetracker.benchmark.job.q16;

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
public class Q16FindWorstCaseJoinOrdering
{
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

    @Benchmark
    public void q161TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query161.class);
    }

    @Benchmark
    public void q161HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query161.class);
    }

    @Benchmark
    public void q161LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query161.class);
    }

    @Benchmark
    public void q161Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query161.class);
    }

    @Benchmark
    public void q162TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query162.class);
    }

    @Benchmark
    public void q162HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query162.class);
    }

    @Benchmark
    public void q162LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query162.class);
    }

    @Benchmark
    public void q162Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query162.class);
    }

    @Benchmark
    public void q163TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query163.class);
    }

    @Benchmark
    public void q163HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query163.class);
    }

    @Benchmark
    public void q163LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query163.class);
    }

    @Benchmark
    public void q163Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query163.class);
    }

    @Benchmark
    public void q164TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query164.class);
    }

    @Benchmark
    public void q164HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query164.class);
    }

    @Benchmark
    public void q164LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query164.class);
    }

    @Benchmark
    public void q164Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query164.class);
    }

    @Benchmark
    public void q165TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query165.class);
    }

    @Benchmark
    public void q165HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query165.class);
    }

    @Benchmark
    public void q165LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query165.class);
    }

    @Benchmark
    public void q165Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query165.class);
    }

    @Benchmark
    public void q166TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query166.class);
    }

    @Benchmark
    public void q166HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query166.class);
    }

    @Benchmark
    public void q166LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query166.class);
    }

    @Benchmark
    public void q166Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query166.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q16FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
