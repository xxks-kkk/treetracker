package org.zhu45.treetracker.benchmark.job.q3;

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
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.benchmarkQuery;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BenchmarkQ3
{
    @Benchmark
    public void q3TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query32.class);
    }

    @Benchmark
    public void q3HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query32.class);
    }

    @Benchmark
    public void q3LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query32.class);
    }

    @Benchmark
    public void q3Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query32.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkQ3.class, JOB_RESULT_STORED_PATH).run();
    }
}
