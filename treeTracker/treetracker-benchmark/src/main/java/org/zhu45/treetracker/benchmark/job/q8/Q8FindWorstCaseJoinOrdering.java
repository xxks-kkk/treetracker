package org.zhu45.treetracker.benchmark.job.q8;

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

/**
 * We want to find the worst-case for hash join and at the same time,
 * the best-case for TTJ.
 */
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class Q8FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q8c1TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query81.class);
    }

    @Benchmark
    public void q8c1HashJoin()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query81.class);
    }

    @Benchmark
    public void q8c1LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query81.class);
    }

    @Benchmark
    public void q8c2TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query82.class);
    }

    @Benchmark
    public void q8c2HashJoin()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query82.class);
    }

    @Benchmark
    public void q8c2LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query82.class);
    }

    @Benchmark
    public void q8c3TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query83.class);
    }

    @Benchmark
    public void q8c3HashJoin()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query83.class);
    }

    @Benchmark
    public void q8c3LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query83.class);
    }

    @Benchmark
    public void q8c4TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query84.class);
    }

    @Benchmark
    public void q8c4HashJoin()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query84.class);
    }

    @Benchmark
    public void q8c4LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query84.class);
    }

    @Benchmark
    public void q8c5TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query85.class);
    }

    @Benchmark
    public void q8c5HashJoin()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query85.class);
    }

    @Benchmark
    public void q8c5LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query85.class);
    }

    @Benchmark
    public void q8c6TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query86.class);
    }

    @Benchmark
    public void q8c6HashJoin()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query86.class);
    }

    @Benchmark
    public void q8c6LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query86.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q8FindWorstCaseJoinOrdering.class).run();
    }
}
