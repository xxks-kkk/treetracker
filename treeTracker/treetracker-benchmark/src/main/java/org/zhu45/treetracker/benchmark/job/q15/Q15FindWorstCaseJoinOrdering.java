package org.zhu45.treetracker.benchmark.job.q15;

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
public class Q15FindWorstCaseJoinOrdering
{
//    @Benchmark
//    public void q15TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query15.class);
//    }
//
//    @Benchmark
//    public void q15HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query15.class);
//    }
//
//    @Benchmark
//    public void q15LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query15.class);
//    }
//
//    @Benchmark
//    public void q15Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query15.class);
//    }
//
//    @Benchmark
//    public void q151TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query151.class);
//    }
//
//    @Benchmark
//    public void q151HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query151.class);
//    }
//
//    @Benchmark
//    public void q151LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query151.class);
//    }
//
//    @Benchmark
//    public void q151Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query151.class);
//    }
//
//    @Benchmark
//    public void q152TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query152.class);
//    }
//
//    @Benchmark
//    public void q152HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query152.class);
//    }
//
//    @Benchmark
//    public void q152LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query152.class);
//    }
//
//    @Benchmark
//    public void q152Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query152.class);
//    }
//
//    @Benchmark
//    public void q153TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query153.class);
//    }
//
//    @Benchmark
//    public void q153HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query153.class);
//    }
//
//    @Benchmark
//    public void q153LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query153.class);
//    }
//
//    @Benchmark
//    public void q153Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query153.class);
//    }
//
//    @Benchmark
//    public void q154TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query154.class);
//    }
//
//    @Benchmark
//    public void q154HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query154.class);
//    }
//
//    @Benchmark
//    public void q154LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query154.class);
//    }
//
//    @Benchmark
//    public void q154Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query154.class);
//    }
//
//    @Benchmark
//    public void q155TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query155.class);
//    }
//
//    @Benchmark
//    public void q155HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query155.class);
//    }
//
//    @Benchmark
//    public void q155LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query155.class);
//    }
//
//    @Benchmark
//    public void q155Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query155.class);
//    }
//
//    @Benchmark
//    public void q156TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query156.class);
//    }
//
//    @Benchmark
//    public void q156HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query156.class);
//    }
//
//    @Benchmark
//    public void q156LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query156.class);
//    }
//
//    @Benchmark
//    public void q156Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query156.class);
//    }
//
//    @Benchmark
//    public void q157TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query157.class);
//    }
//
//    @Benchmark
//    public void q157HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query157.class);
//    }
//
//    @Benchmark
//    public void q157LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query157.class);
//    }
//
//    @Benchmark
//    public void q157Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query157.class);
//    }
//
//    @Benchmark
//    public void q158TTJ()
//    {
//        benchmarkQuery(JoinOperator.TTJHP, Query158.class);
//    }
//
//    @Benchmark
//    public void q158HJ()
//    {
//        benchmarkQuery(JoinOperator.HASH_JOIN, Query158.class);
//    }
//
//    @Benchmark
//    public void q158LIP()
//    {
//        benchmarkQuery(JoinOperator.LIP, Query158.class);
//    }
//
//    @Benchmark
//    public void q158Yannakakis()
//    {
//        benchmarkQuery(JoinOperator.Yannakakis, Query158.class);
//    }

    @Benchmark
    public void q1510TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query1510.class);
    }

    @Benchmark
    public void q1510HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query1510.class);
    }

    @Benchmark
    public void q1510LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query1510.class);
    }

    @Benchmark
    public void q1510Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query1510.class);
    }

    @Benchmark
    public void q1511TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query1511.class);
    }

    @Benchmark
    public void q1511HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query1511.class);
    }

    @Benchmark
    public void q1511LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query1511.class);
    }

    @Benchmark
    public void q1511Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query1511.class);
    }

    @Benchmark
    public void q1512TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, Query1512.class);
    }

    @Benchmark
    public void q1512HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, Query1512.class);
    }

    @Benchmark
    public void q1512LIP()
    {
        benchmarkQuery(JoinOperator.LIP, Query1512.class);
    }

    @Benchmark
    public void q1512Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, Query1512.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(Q15FindWorstCaseJoinOrdering.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
