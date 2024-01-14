package org.zhu45.treetracker.benchmark.job.q4;

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
public class Q4FindWorstCaseJoinOrdering
{
    @Benchmark
    public void q41TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query41.class);
    }

    @Benchmark
    public void q41HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query41.class);
    }

    @Benchmark
    public void q41LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query41.class);
    }

    @Benchmark
    public void q41Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query41.class);
    }

    @Benchmark
    public void q42TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query42.class);
    }

    @Benchmark
    public void q42HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query42.class);
    }

    @Benchmark
    public void q42LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query42.class);
    }

    @Benchmark
    public void q42Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query42.class);
    }

    @Benchmark
    public void q43TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query43.class);
    }

    @Benchmark
    public void q43HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query43.class);
    }

    @Benchmark
    public void q43LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query43.class);
    }

    @Benchmark
    public void q43Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query43.class);
    }

    @Benchmark
    public void q44TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query44.class);
    }

    @Benchmark
    public void q44HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query44.class);
    }

    @Benchmark
    public void q44LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query44.class);
    }

    @Benchmark
    public void q44Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query44.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q4FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
