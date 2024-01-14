package org.zhu45.treetracker.benchmark.job.q6;

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
public class Q6FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q61TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query61.class);
    }

    @Benchmark
    public void q61HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query61.class);
    }

    @Benchmark
    public void q61LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query61.class);
    }

    @Benchmark
    public void q61Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query61.class);
    }

    @Benchmark
    public void q62TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query62.class);
    }

    @Benchmark
    public void q62HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query62.class);
    }

    @Benchmark
    public void q62LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query62.class);
    }

    @Benchmark
    public void q62Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query62.class);
    }

    @Benchmark
    public void q63TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query63.class);
    }

    @Benchmark
    public void q63HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query63.class);
    }

    @Benchmark
    public void q63LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query63.class);
    }

    @Benchmark
    public void q63Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query63.class);
    }

    @Benchmark
    public void q64TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query64.class);
    }

    @Benchmark
    public void q64HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query64.class);
    }

    @Benchmark
    public void q64LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query64.class);
    }

    @Benchmark
    public void q64Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query64.class);
    }

    @Benchmark
    public void q65TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query65.class);
    }

    @Benchmark
    public void q65HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query65.class);
    }

    @Benchmark
    public void q65LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query65.class);
    }

    @Benchmark
    public void q65Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query65.class);
    }

    @Benchmark
    public void q66TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query66.class);
    }

    @Benchmark
    public void q66HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query66.class);
    }

    @Benchmark
    public void q66LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query66.class);
    }

    @Benchmark
    public void q66Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query66.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q6FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
