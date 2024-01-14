package org.zhu45.treetracker.benchmark.job.q7;

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
public class Q7bFindWorstCaseJoinOrdering
{
    @Benchmark
    public void q71TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query7b1.class);
    }

    @Benchmark
    public void q71HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query7b1.class);
    }

    @Benchmark
    public void q71LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query7b1.class);
    }

    @Benchmark
    public void q71Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query7b1.class);
    }

    @Benchmark
    public void q72TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query7b2.class);
    }

    @Benchmark
    public void q72HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query7b2.class);
    }

    @Benchmark
    public void q72LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query7b2.class);
    }

    @Benchmark
    public void q72Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query7b2.class);
    }

    @Benchmark
    public void q73TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query7b3.class);
    }

    @Benchmark
    public void q73HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query7b3.class);
    }

    @Benchmark
    public void q73LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query7b3.class);
    }

    @Benchmark
    public void q73Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query7b3.class);
    }

    @Benchmark
    public void q74TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query7b4.class);
    }

    @Benchmark
    public void q74HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query7b4.class);
    }

    @Benchmark
    public void q74LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query7b4.class);
    }

    @Benchmark
    public void q74Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query7b4.class);
    }

    @Benchmark
    public void q75TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query7b5.class);
    }

    @Benchmark
    public void q75HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query7b5.class);
    }

    @Benchmark
    public void q75LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query7b5.class);
    }

    @Benchmark
    public void q75Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query7b5.class);
    }

    @Benchmark
    public void q76TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query7b6.class);
    }

    @Benchmark
    public void q76HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query7b6.class);
    }

    @Benchmark
    public void q76LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query7b6.class);
    }

    @Benchmark
    public void q76Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query7b6.class);
    }

    @Benchmark
    public void q77TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query7b7.class);
    }

    @Benchmark
    public void q77HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query7b7.class);
    }

    @Benchmark
    public void q77LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query7b7.class);
    }

    @Benchmark
    public void q77Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query7b7.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q7bFindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
