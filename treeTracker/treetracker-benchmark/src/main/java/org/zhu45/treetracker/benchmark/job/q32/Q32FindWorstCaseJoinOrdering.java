package org.zhu45.treetracker.benchmark.job.q32;

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
public class Q32FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q321TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query321.class);
    }

    @Benchmark
    public void q321HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query321.class);
    }

    @Benchmark
    public void q321LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query321.class);
    }

    @Benchmark
    public void q321Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query321.class);
    }

    @Benchmark
    public void q322TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query322.class);
    }

    @Benchmark
    public void q322HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query322.class);
    }

    @Benchmark
    public void q322LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query322.class);
    }

    @Benchmark
    public void q322Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query322.class);
    }

    @Benchmark
    public void q323TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query323.class);
    }

    @Benchmark
    public void q323HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query323.class);
    }

    @Benchmark
    public void q323LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query323.class);
    }

    @Benchmark
    public void q323Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query323.class);
    }

    @Benchmark
    public void q324TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query324.class);
    }

    @Benchmark
    public void q324HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query324.class);
    }

    @Benchmark
    public void q324LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query324.class);
    }

    @Benchmark
    public void q324Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query324.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q32FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
