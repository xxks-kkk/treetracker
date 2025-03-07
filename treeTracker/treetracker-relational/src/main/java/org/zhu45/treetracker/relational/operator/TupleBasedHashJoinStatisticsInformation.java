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
    // number of hash table probe
    @Getter private int numberOfHashTableProbe;
    // number of semijoin probe success. Effectively, this outputs
    // semijoin output size
    @Getter private long numberOfSemiJoinSuccess;
    private long hashTableSizeAfterEvaluation;

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

    @Override
    public void incrementNumberOfHashTableProbe()
    {
        this.numberOfHashTableProbe++;
    }

    @Override
    public long getNumberOfHashTableProbe()
    {
        return this.numberOfHashTableProbe;
    }

    @Override
    public void incrementNumberOfSemiJoinSuccess()
    {
        this.numberOfSemiJoinSuccess++;
    }

    @Override
    public long getNumberOfSemiJoinSuccess()
    {
        return this.numberOfSemiJoinSuccess;
    }

    @Override
    public void updateHashTableSizeAfterEvaluation(long hashTableSize)
    {
        this.hashTableSizeAfterEvaluation = hashTableSize;
    }

    @Override
    public long getHashTableSizeAfterEvaluation()
    {
        return hashTableSizeAfterEvaluation;
    }
}
