package org.zhu45.treetracker.benchmark.job.q33;

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
public class Q33FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q331TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query331.class);
    }

    @Benchmark
    public void q331HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query331.class);
    }

    @Benchmark
    public void q331LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query331.class);
    }

    @Benchmark
    public void q331Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query331.class);
    }

    @Benchmark
    public void q332TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query332.class);
    }

    @Benchmark
    public void q332HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query332.class);
    }

    @Benchmark
    public void q332LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query332.class);
    }

    @Benchmark
    public void q332Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query332.class);
    }

    @Benchmark
    public void q333TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query333.class);
    }

    @Benchmark
    public void q333HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query333.class);
    }

    @Benchmark
    public void q333LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query333.class);
    }

    @Benchmark
    public void q333Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query333.class);
    }

    @Benchmark
    public void q334TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query334.class);
    }

    @Benchmark
    public void q334HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query334.class);
    }

    @Benchmark
    public void q334LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query334.class);
    }

    @Benchmark
    public void q334Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query334.class);
    }

    @Benchmark
    public void q335TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query335.class);
    }

    @Benchmark
    public void q335HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query335.class);
    }

    @Benchmark
    public void q335LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query335.class);
    }

    @Benchmark
    public void q335Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query335.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q33FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
