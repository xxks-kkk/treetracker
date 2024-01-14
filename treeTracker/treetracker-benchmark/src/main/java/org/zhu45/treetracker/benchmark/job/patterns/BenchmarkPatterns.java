package org.zhu45.treetracker.benchmark.job.patterns;

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
public class BenchmarkPatterns
{
    @Benchmark
    public void castInfoPatternTTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, CastInfoPattern.class);
    }

    @Benchmark
    public void castInfoPatternHJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, CastInfoPattern.class);
    }

    @Benchmark
    public void castInfoPatternLIP()
    {
        benchmarkQuery(JoinOperator.LIP, CastInfoPattern.class);
    }

    @Benchmark
    public void castInfoPatternYannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, CastInfoPattern.class);
    }

    @Benchmark
    public void castInfoPattern2TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, CastInfoPattern2.class);
    }

    @Benchmark
    public void castInfoPattern2HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, CastInfoPattern2.class);
    }

    @Benchmark
    public void castInfoPattern2LIP()
    {
        benchmarkQuery(JoinOperator.LIP, CastInfoPattern2.class);
    }

    @Benchmark
    public void castInfoPattern2Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, CastInfoPattern2.class);
    }

    @Benchmark
    public void castInfoPattern3TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, CastInfoPattern3.class);
    }

    @Benchmark
    public void castInfoPattern3HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, CastInfoPattern3.class);
    }

    @Benchmark
    public void castInfoPattern3LIP()
    {
        benchmarkQuery(JoinOperator.LIP, CastInfoPattern3.class);
    }

    @Benchmark
    public void castInfoPattern3Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, CastInfoPattern3.class);
    }

    @Benchmark
    public void movieCompaniesPatternTTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, MovieCompaniesPattern.class);
    }

    @Benchmark
    public void movieCompaniesPatternHJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, MovieCompaniesPattern.class);
    }

    @Benchmark
    public void movieCompaniesPatternLIP()
    {
        benchmarkQuery(JoinOperator.LIP, MovieCompaniesPattern.class);
    }

    @Benchmark
    public void movieCompaniesPatternYannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, MovieCompaniesPattern.class);
    }

    @Benchmark
    public void movieCompaniesPattern1TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, MovieCompaniesPattern1.class);
    }

    @Benchmark
    public void movieCompaniesPattern1HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, MovieCompaniesPattern1.class);
    }

    @Benchmark
    public void movieCompaniesPattern1LIP()
    {
        benchmarkQuery(JoinOperator.LIP, MovieCompaniesPattern1.class);
    }

    @Benchmark
    public void movieCompaniesPattern1Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, MovieCompaniesPattern1.class);
    }

    @Benchmark
    public void movieCompaniesPattern2TTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, MovieCompaniesPattern2.class);
    }

    @Benchmark
    public void movieCompaniesPattern2HJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, MovieCompaniesPattern2.class);
    }

    @Benchmark
    public void movieCompaniesPattern2LIP()
    {
        benchmarkQuery(JoinOperator.LIP, MovieCompaniesPattern2.class);
    }

    @Benchmark
    public void movieCompaniesPattern2Yannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, MovieCompaniesPattern2.class);
    }

    @Benchmark
    public void titlePatternTTJ()
    {
        benchmarkQuery(JoinOperator.TTJHP, TitlePattern.class);
    }

    @Benchmark
    public void titlePatternHJ()
    {
        benchmarkQuery(JoinOperator.HASH_JOIN, TitlePattern.class);
    }

    @Benchmark
    public void titlePatternLIP()
    {
        benchmarkQuery(JoinOperator.LIP, TitlePattern.class);
    }

    @Benchmark
    public void titlePatternYannakakis()
    {
        benchmarkQuery(JoinOperator.Yannakakis, TitlePattern.class);
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkPatterns.class, OTHERS_RESULT_STORED_PATH).run();
    }
}
