package org.zhu45.treetracker.relational.operator;

import org.zhu45.treetracker.common.Domain;

import java.util.Map;

public abstract class AbstractStatisticsInformation
        implements OperatorInformation, StatisticsInformation
{
    public void updateNumberOfTuplesRemovedByFullReducer(String relationName, long tuplesRemoved)
    {
        throw new UnsupportedOperationException();
    }

    public Map<String, Long> getNumberOfTuplesRemovedByFullReducer()
    {
        throw new UnsupportedOperationException();
    }

    public Domain getDomain()
    {
        throw new UnsupportedOperationException();
    }

    public int getDomainSize()
    {
        throw new UnsupportedOperationException();
    }

    public void setDomainSize(int domainSize)
    {
        throw new UnsupportedOperationException();
    }

    public void setDomain(Domain domain)
    {
        throw new UnsupportedOperationException();
    }

    public int getNumberOfNoGoodTuplesFiltered()
    {
        throw new UnsupportedOperationException();
    }

    public void setNumberOfNoGoodTuples(int numberOfNoGoodTuples)
    {
        throw new UnsupportedOperationException();
    }

    public void incrementNumberOfNoGoodTuples()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfDanglingTuples()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incrementNumberOfDanglingTuples()
    {
        throw new UnsupportedOperationException();
    }

    public int getNumberOfNoGoodTuples()
    {
        throw new UnsupportedOperationException();
    }

    public void incrementNumberOfNoGoodTuplesFiltered()
    {
        throw new UnsupportedOperationException();
    }

    public String getNoGoodListMapRepresentation()
    {
        throw new UnsupportedOperationException();
    }

    public void setNoGoodListMapRepresentation(String noGoodListMapRepresentation)
    {
        throw new UnsupportedOperationException();
    }

    public void incrementNumberOfR1Assignments()
    {
        throw new UnsupportedOperationException();
    }

    public long getNumberOfR1Assignments()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNumberOfR1Assignments(long numberOfR1Assignments)
    {
        throw new UnsupportedOperationException();
    }

    public void setNumberOfTuples(long numberOfTuples)
    {
        throw new UnsupportedOperationException();
    }

    public long getNumberOfTuples()
    {
        throw new UnsupportedOperationException();
    }

    public void setNumberOfBloomFiltersRegistered(int numberOfBloomFiltersRegistered)
    {
        throw new UnsupportedOperationException();
    }

    public int getNumberOfBloomFiltersRegistered()
    {
        throw new UnsupportedOperationException();
    }

    public long getApproxNumberOfTuplesInBloomFilter()
    {
        throw new UnsupportedOperationException();
    }

    public int getNumberOfTuplesFilteredOutByBloomFilters()
    {
        throw new UnsupportedOperationException();
    }

    public void incrementNumberOfTuplesFilterOutByBloomFilters()
    {
        throw new UnsupportedOperationException();
    }

    public void setApproxNumberOfTuplesInBloomFilter(long approxNumberOfTuplesInBloomFilter)
    {
        throw new UnsupportedOperationException();
    }

    public void setBuildBloomFiltersTime(long buildBloomFiltersTime)
    {
        throw new UnsupportedOperationException();
    }

    public long getBuildBloomFiltersTime()
    {
        throw new UnsupportedOperationException();
    }

    public void setBloomFiltersProbingTime(long bloomFiltersProbingTime)
    {
        throw new UnsupportedOperationException();
    }

    public long getBloomFiltersProbingTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getFetchingTuplesTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFetchingTuplesTime(long fetchingTuplesTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNoGoodListMapKeyNum(int noGoodListMapKeyNum)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNoGoodListProbingTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNoGoodListProbingTime(long noGoodListProbingTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateJoinTime(long joinTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJoinTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updatePassContextWorkTime(long passContextWorkTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPassContextWorkTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHashTableBuildTime(long hashTableBuildTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getHashTableBuildTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDeleteDanglingTupleFromHTime(long deleteDanglingTupleFromHTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getDeleteDanglingTupleFromHTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBuildNoGoodListTime(long buildNoGoodListTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getBuildNoGoodListTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setNoGoodListConstructTime(long noGoodListConstructTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNoGoodListConstructTime()
    {
        throw new UnsupportedOperationException();
    }

    public void setNoGoodListSizeInBytes(long noGoodListSizeInBytes)
    {
        throw new UnsupportedOperationException();
    }

    public long getNoGoodListSizeInBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateProbeHashTableTime(long probeHashTableTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getProbeHashTableTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setHashTableInitialAllocationCapacity(int hashTableInitialAllocationCapacity)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHashTableInitialAllocationCapacity()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getRecordTupleSourceClazzName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRecordTupleSourceClazzName(String recordTupleSourceClazzName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPredicateEvaluationTime()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPredicateEvaluationTime(long predicateEvaluationTime)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incrementNumberOfPassContextCalls()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfPassContextCalls()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void incrementNumberOfInitPassContextCalls()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNumberOfInitPassContextCalls()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSummationOfSemijoinOutputSize(long summationOfSemijoinOutputSize)
    {
        throw new UnsupportedOperationException();
    }
}
