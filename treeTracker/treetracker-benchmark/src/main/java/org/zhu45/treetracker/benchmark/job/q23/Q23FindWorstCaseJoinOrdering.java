package org.zhu45.treetracker.benchmark.job.q23;

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
public class Q23FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q232TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query232.class);
    }

    @Benchmark
    public void q232HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query232.class);
    }

    @Benchmark
    public void q232LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query232.class);
    }

    @Benchmark
    public void q232Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query232.class);
    }

    @Benchmark
    public void q233TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query233.class);
    }

    @Benchmark
    public void q233HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query233.class);
    }

    @Benchmark
    public void q233LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query233.class);
    }

    @Benchmark
    public void q233Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query233.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q23FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
