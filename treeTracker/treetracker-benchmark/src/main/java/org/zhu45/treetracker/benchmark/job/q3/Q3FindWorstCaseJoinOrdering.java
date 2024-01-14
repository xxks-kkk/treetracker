package org.zhu45.treetracker.benchmark.job.q3;

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
public class Q3FindWorstCaseJoinOrdering
{
//    @Benchmark
//    public void q31TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query31.class);
//    }
//
//    @Benchmark
//    public void q31HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query31.class);
//    }
//
//    @Benchmark
//    public void q31LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query31.class);
//    }
//
//    @Benchmark
//    public void q31Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query31.class);
//    }
//
//    @Benchmark
//    public void q32TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query32.class);
//    }
//
//    @Benchmark
//    public void q32HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query32.class);
//    }
//
//    @Benchmark
//    public void q32LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query32.class);
//    }
//
//    @Benchmark
//    public void q32Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query32.class);
//    }
//
//    @Benchmark
//    public void q33TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query33.class);
//    }
//
//    @Benchmark
//    public void q33HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query33.class);
//    }
//
//    @Benchmark
//    public void q33LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query33.class);
//    }
//
//    @Benchmark
//    public void q33Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query33.class);
//    }

    @Benchmark
    public void q34TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query34.class);
    }

    @Benchmark
    public void q34HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query34.class);
    }

    @Benchmark
    public void q34LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query34.class);
    }

    @Benchmark
    public void q34Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query34.class);
    }

    @Benchmark
    public void q35TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query35.class);
    }

    @Benchmark
    public void q35HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query35.class);
    }

    @Benchmark
    public void q35LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query35.class);
    }

    @Benchmark
    public void q35Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query35.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q3FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
