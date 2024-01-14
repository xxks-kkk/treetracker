package org.zhu45.treetracker.relational.operator;

import lombok.Getter;

@Getter
public class TupleBasedLIPHashJoinStatisticsInformation
        extends TupleBasedHashJoinStatisticsInformation
{
    // (approx) number of tuples in the Bloom filters (LIP specific)
    private long approxNumberOfTuplesInBloomFilter;
    // time takes to build Bloom filters
    private long buildBloomFiltersTime;

    public void setApproxNumberOfTuplesInBloomFilter(long approxNumberOfTuplesInBloomFilter)
    {
        this.approxNumberOfTuplesInBloomFilter = approxNumberOfTuplesInBloomFilter;
    }

    public void setBuildBloomFiltersTime(long buildBloomFiltersTime)
    {
        this.buildBloomFiltersTime = buildBloomFiltersTime;
    }
}
