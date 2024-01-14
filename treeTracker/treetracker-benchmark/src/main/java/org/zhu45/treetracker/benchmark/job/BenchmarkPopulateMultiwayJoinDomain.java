package org.zhu45.treetracker.benchmark.job;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitle;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
public class BenchmarkPopulateMultiwayJoinDomain
{
    @Benchmark
    public void populateWithVarchar()
    {
        MultiwayJoinNode titleVarchar = getTitle();
        try {
            titleVarchar.populateDomain();
        }
        finally {
            ((MultiwayJoinDomain) titleVarchar.getDomain()).close();
        }
    }

    @Benchmark
    public void populateWithInt()
    {
        MultiwayJoinNode titleInt = getTitleInt(JOBQueries.NOPREDICATE, null);
        try {
            titleInt.populateDomain();
        }
        finally {
            ((MultiwayJoinDomain) titleInt.getDomain()).close();
        }
    }

    @Test
    public void verification()
    {
        MultiwayJoinNode titleVarchar = getTitleInt(JOBQueries.NOPREDICATE, null);
        try {
            titleVarchar.populateDomain();
            assertEquals(titleVarchar.getDomain().size(), 2528312);
        }
        finally {
            ((MultiwayJoinDomain) titleVarchar.getDomain()).close();
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkPopulateMultiwayJoinDomain.class).run();
    }
}
