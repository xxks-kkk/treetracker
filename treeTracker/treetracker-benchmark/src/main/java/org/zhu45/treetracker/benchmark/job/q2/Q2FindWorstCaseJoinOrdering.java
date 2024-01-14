package org.zhu45.treetracker.benchmark.job.q2;

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
public class Q2FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q21TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query21.class);
    }

    @Benchmark
    public void q21HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query21.class);
    }

    @Benchmark
    public void q21LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query21.class);
    }

    @Benchmark
    public void q21Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query21.class);
    }

    @Benchmark
    public void q22TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query22.class);
    }

    @Benchmark
    public void q22HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query22.class);
    }

    @Benchmark
    public void q22LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query22.class);
    }

    @Benchmark
    public void q22Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query22.class);
    }

    @Benchmark
    public void q23TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query23.class);
    }

    @Benchmark
    public void q23HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query23.class);
    }

    @Benchmark
    public void q23LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query23.class);
    }

    @Benchmark
    public void q23Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query23.class);
    }

    @Benchmark
    public void q24TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query24.class);
    }

    @Benchmark
    public void q24HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query24.class);
    }

    @Benchmark
    public void q24LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query24.class);
    }

    @Benchmark
    public void q24Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query24.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q2FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
