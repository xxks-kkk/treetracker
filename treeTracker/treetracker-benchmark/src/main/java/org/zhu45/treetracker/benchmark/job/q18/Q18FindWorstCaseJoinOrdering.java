package org.zhu45.treetracker.benchmark.job.q18;

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
public class Q18FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q181TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query181.class);
    }

    @Benchmark
    public void q181HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query181.class);
    }

    @Benchmark
    public void q181LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query181.class);
    }

    @Benchmark
    public void q181Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query181.class);
    }

    @Benchmark
    public void q182TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query182.class);
    }

    @Benchmark
    public void q182HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query182.class);
    }

    @Benchmark
    public void q182LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query182.class);
    }

    @Benchmark
    public void q182Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query182.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q18FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
