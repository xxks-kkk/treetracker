package org.zhu45.treetracker.relational.operator;

import org.zhu45.treetracker.common.Domain;

import java.util.Map;

public interface StatisticsInformation
{
    void updateNumberOfTuplesRemovedByFullReducer(String relationName, long tuplesRemoved);

    Map<String, Long> getNumberOfTuplesRemovedByFullReducer();

    Domain getDomain();

    int getDomainSize();

    void setDomain(Domain domain);

    void setDomainSize(int domainSize);

    void setNumberOfNoGoodTuples(int numberOfNoGoodTuples);

    void incrementNumberOfNoGoodTuples();

    int getNumberOfNoGoodTuples();

    void incrementNumberOfDanglingTuples();

    long getNumberOfDanglingTuples();

    void incrementNumberOfNoGoodTuplesFiltered();

    int getNumberOfNoGoodTuplesFiltered();

    String getNoGoodListMapRepresentation();

    void setNoGoodListMapRepresentation(String noGoodListMapRepresentation);

    void setNoGoodListMapKeyNum(int noGoodListMapKeyNum);

    void incrementNumberOfR1Assignments();

    long getNumberOfR1Assignments();

    void updateNumberOfR1Assignments(long numberOfR1Assignments);

    void setNumberOfTuples(long numberOfTuples);

    void setNumberOfBloomFiltersRegistered(int numberOfBloomFiltersRegistered);

    int getNumberOfBloomFiltersRegistered();

    long getApproxNumberOfTuplesInBloomFilter();

    void setApproxNumberOfTuplesInBloomFilter(long approxNumberOfTuplesInBloomFilter);

    void setBuildBloomFiltersTime(long buildBloomFiltersTime);

    long getBuildBloomFiltersTime();

    void setBloomFiltersProbingTime(long bloomFiltersProbingTime);

    long getBloomFiltersProbingTime();

    void incrementNumberOfTuplesFilterOutByBloomFilters();

    int getNumberOfTuplesFilteredOutByBloomFilters();

    long getNumberOfTuples();

    void setFetchingTuplesTime(long fetchingTuplesTime);

    long getFetchingTuplesTime();

    void setNoGoodListProbingTime(long noGoodListProbingTime);

    long getNoGoodListProbingTime();

    void updateJoinTime(long joinTime);

    long getJoinTime();

    void updatePassContextWorkTime(long passContextWorkTime);

    long getPassContextWorkTime();

    void incrementNumberOfPassContextCalls();

    long getNumberOfPassContextCalls();

    void incrementNumberOfInitPassContextCalls();

    long getNumberOfInitPassContextCalls();

    void updateDeleteDanglingTupleFromHTime(long deleteDanglingTupleFromHTime);

    long getDeleteDanglingTupleFromHTime();

    void setHashTableBuildTime(long hashTableBuildTime);

    long getHashTableBuildTime();

    void updateBuildNoGoodListTime(long buildNoGoodListTime);

    long getBuildNoGoodListTime();

    void setNoGoodListConstructTime(long noGoodListConstructTime);

    long getNoGoodListConstructTime();

    void setNoGoodListSizeInBytes(long noGoodListSizeInBytes);

    long getNoGoodListSizeInBytes();

    void updateProbeHashTableTime(long probeHashTableTime);

    long getProbeHashTableTime();

    void setHashTableInitialAllocationCapacity(int hashTableInitialAllocationCapacity);

    int getHashTableInitialAllocationCapacity();

    void setRecordTupleSourceClazzName(String recordTupleSourceClazzName);

    String getRecordTupleSourceClazzName();

    long getPredicateEvaluationTime();

    void setPredicateEvaluationTime(long predicateEvaluationTime);

    void updateSummationOfSemijoinOutputSize(long summationOfSemijoinOutputSize);
}
