package org.zhu45.treetracker.benchmark.job.q17;

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
public class Q17FindWorstCaseJoinOrdering
{
//    @Benchmark
//    public void q17TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query17.class);
//    }
//
//    @Benchmark
//    public void q17HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query17.class);
//    }
//
//    @Benchmark
//    public void q17LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query17.class);
//    }
//
//    @Benchmark
//    public void q17Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query17.class);
//    }
//
//    @Benchmark
//    public void q171TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query171.class);
//    }
//
//    @Benchmark
//    public void q171HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query171.class);
//    }
//
//    @Benchmark
//    public void q171LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query171.class);
//    }
//
//    @Benchmark
//    public void q171Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query171.class);
//    }
//
//    @Benchmark
//    public void q172TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query172.class);
//    }
//
//    @Benchmark
//    public void q172HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query172.class);
//    }
//
//    @Benchmark
//    public void q172LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query172.class);
//    }
//
//    @Benchmark
//    public void q172Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query172.class);
//    }
//
//    @Benchmark
//    public void q173TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query173.class);
//    }
//
//    @Benchmark
//    public void q173HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query173.class);
//    }
//
//    @Benchmark
//    public void q173LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query173.class);
//    }
//
//    @Benchmark
//    public void q173Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query173.class);
//    }
//
//    @Benchmark
//    public void q174TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query174.class);
//    }
//
//    @Benchmark
//    public void q174HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query174.class);
//    }
//
//    @Benchmark
//    public void q174LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query174.class);
//    }
//
//    @Benchmark
//    public void q174Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query174.class);
//    }
//
//    @Benchmark
//    public void q175TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query175.class);
//    }
//
//    @Benchmark
//    public void q175HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query175.class);
//    }
//
//    @Benchmark
//    public void q175LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query175.class);
//    }
//
//    @Benchmark
//    public void q175Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query175.class);
//    }
//
//    @Benchmark
//    public void q176TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query176.class);
//    }
//
//    @Benchmark
//    public void q176HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query176.class);
//    }
//
//    @Benchmark
//    public void q176LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query176.class);
//    }
//
//    @Benchmark
//    public void q176Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query176.class);
//    }

    @Benchmark
    public void q178TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query178.class);
    }

    @Benchmark
    public void q178HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query178.class);
    }

    @Benchmark
    public void q178LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query178.class);
    }

    @Benchmark
    public void q178Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query178.class);
    }

    @Benchmark
    public void q179TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query179.class);
    }

    @Benchmark
    public void q179HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query179.class);
    }

    @Benchmark
    public void q179LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query179.class);
    }

    @Benchmark
    public void q179Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query179.class);
    }

    @Benchmark
    public void q1710TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query1710.class);
    }

    @Benchmark
    public void q1710HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query1710.class);
    }

    @Benchmark
    public void q1710LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query1710.class);
    }

    @Benchmark
    public void q1710Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query1710.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q17FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
