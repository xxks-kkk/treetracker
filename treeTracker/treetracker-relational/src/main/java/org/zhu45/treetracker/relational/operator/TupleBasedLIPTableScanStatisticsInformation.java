package org.zhu45.treetracker.relational.operator;

import lombok.Getter;

@Getter
public class TupleBasedLIPTableScanStatisticsInformation
        extends TupleBasedTableScanStatisticsInformation
{
    // number of Bloom filters registered in the current operator (LIP specific)
    private int numberOfBloomFiltersRegistered;
    // number of tuples filtered out due to Bloom filters (LIP specific)
    private int numberOfTuplesFilteredOutByBloomFilters;
    // Bloom filters probing time
    private long bloomFiltersProbingTime;

    public void incrementNumberOfTuplesFilterOutByBloomFilters()
    {
        this.numberOfTuplesFilteredOutByBloomFilters += 1;
    }

    public void setNumberOfBloomFiltersRegistered(int numberOfBloomFiltersRegistered)
    {
        this.numberOfBloomFiltersRegistered = numberOfBloomFiltersRegistered;
    }

    public void setBloomFiltersProbingTime(long bloomFiltersProbingTime)
    {
        this.bloomFiltersProbingTime = bloomFiltersProbingTime;
    }
}
