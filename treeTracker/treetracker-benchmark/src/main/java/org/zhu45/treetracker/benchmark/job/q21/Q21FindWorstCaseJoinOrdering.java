package org.zhu45.treetracker.benchmark.job.q21;

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
public class Q21FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q211TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query211.class);
    }

    @Benchmark
    public void q211HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query211.class);
    }

    @Benchmark
    public void q211LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query211.class);
    }

    @Benchmark
    public void q211Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query211.class);
    }

    @Benchmark
    public void q212TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query212.class);
    }

    @Benchmark
    public void q212HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query212.class);
    }

    @Benchmark
    public void q212LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query212.class);
    }

    @Benchmark
    public void q212Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query212.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q21FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
