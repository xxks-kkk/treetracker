package org.zhu45.treetracker.benchmark.job.q5;

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
public class Q5FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q51TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query51.class);
    }

    @Benchmark
    public void q51HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query51.class);
    }

    @Benchmark
    public void q51LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query51.class);
    }

    @Benchmark
    public void q51Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query51.class);
    }

    @Benchmark
    public void q52TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query52.class);
    }

    @Benchmark
    public void q52HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query52.class);
    }

    @Benchmark
    public void q52LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query52.class);
    }

    @Benchmark
    public void q52Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query52.class);
    }

    @Benchmark
    public void q53TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query53.class);
    }

    @Benchmark
    public void q53HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query53.class);
    }

    @Benchmark
    public void q53LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query53.class);
    }

    @Benchmark
    public void q53Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query53.class);
    }

    @Benchmark
    public void q54TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query54.class);
    }

    @Benchmark
    public void q54HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query54.class);
    }

    @Benchmark
    public void q54LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query54.class);
    }

    @Benchmark
    public void q54Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query54.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q5FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
