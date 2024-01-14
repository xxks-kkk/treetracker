package org.zhu45.treetracker.benchmark.job.q31;

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
public class Q31FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q311TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query311.class);
    }

    @Benchmark
    public void q311HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query311.class);
    }

    @Benchmark
    public void q311LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query311.class);
    }

    @Benchmark
    public void q311Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query311.class);
    }

    @Benchmark
    public void q312TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query312.class);
    }

    @Benchmark
    public void q312HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query312.class);
    }

    @Benchmark
    public void q312LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query312.class);
    }

    @Benchmark
    public void q312Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query312.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q31FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
