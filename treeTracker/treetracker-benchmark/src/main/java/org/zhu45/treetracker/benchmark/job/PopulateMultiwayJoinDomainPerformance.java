package org.zhu45.treetracker.benchmark.job;

import org.openjdk.jmh.runner.RunnerException;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;

import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;

public class PopulateMultiwayJoinDomainPerformance
{
    private PopulateMultiwayJoinDomainPerformance() {}

    public static void main(String[] args)
            throws RunnerException
    {
        long timeNow = System.currentTimeMillis();
        MultiwayJoinNode titleInt = getCastInfoInt(JOBQueries.NOPREDICATE);
        try {
            titleInt.populateDomain();
        }
        catch (OutOfMemoryError error) {
            System.err.println("number of tuples read so far: " + titleInt.getDomain().size());
            System.err.println("Max JVM memory: " + Runtime.getRuntime().maxMemory());
        }
        finally {
            System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            ((MultiwayJoinDomain) titleInt.getDomain()).close();
        }
    }
}
