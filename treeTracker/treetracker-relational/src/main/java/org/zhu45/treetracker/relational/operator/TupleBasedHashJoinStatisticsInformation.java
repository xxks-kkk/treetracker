package org.zhu45.treetracker.relational.operator;

import lombok.Getter;

public class TupleBasedHashJoinStatisticsInformation
        extends TupleBasedJoinStatisticsInformation
{
    // time spent on building hash table
    @Getter private long hashTableBuildTime;
    // time spent on probing hash table
    @Getter private long probeHashTableTime;
    // hash table initial allocation capacity
    @Getter private int hashTableInitialAllocationCapacity;

    @Override
    public void setHashTableBuildTime(long hashTableBuildTime)
    {
        this.hashTableBuildTime = hashTableBuildTime;
    }

    @Override
    public void updateProbeHashTableTime(long probeHashTableTime)
    {
        this.probeHashTableTime += probeHashTableTime;
    }

    @Override
    public void setHashTableInitialAllocationCapacity(int hashTableInitialAllocationCapacity)
    {
        this.hashTableInitialAllocationCapacity = hashTableInitialAllocationCapacity;
    }
}
