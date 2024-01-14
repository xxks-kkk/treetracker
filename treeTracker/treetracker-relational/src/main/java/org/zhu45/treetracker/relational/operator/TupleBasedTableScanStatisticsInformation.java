package org.zhu45.treetracker.relational.operator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.zhu45.treetracker.common.Domain;

public class TupleBasedTableScanStatisticsInformation
        extends TTJStatisticsInformation
{
    // The maximum size of Domain allowed in StatisticsInformation.
    // If the Domain is too large (i.e., larger than this number),
    // we only track the domain size instead of domain itself
    public static final int maxDomainSize = 10;

    @JsonIgnore
    // domain inside the Table Scan operator
    private Domain domain;
    // domain size inside the Table Scan operator
    private int domainSize;
    // number of tuples in the relation
    private long numberOfTuples;
    // time takes for the underlying data source to evaluate the pushdown predicates
    private long predicateEvaluationTime;

    // number of tuples filtered out due to they have identified as no-good before any computation (TTJ specific)
    private int numberOfNoGoodTuplesFiltered;
    // the string representation of noGoodListMap
    private String noGoodListMapRepresentation;
    // time spent on probing no-good list at R_k
    private long noGoodListProbingTime;
    // size of noGoodList (in bytes)
    private long noGoodListSizeInBytes;

    // the amount of time spent by fetching tuples from the data source
    private long fetchingTuplesTime;

    // RecordTupleSourceProvider (i.e., RecordTupleSource & Row) used
    private String recordTupleSourceProviderClazzName;
    // number of passContext calls on the normal table scan operator's passContext(),
    // which happens in TTJHP_NO_NG
    private long numberOfPassContextCalls;

    public Domain getDomain()
    {
        return domain;
    }

    public int getDomainSize()
    {
        return domainSize;
    }

    public void setDomain(Domain domain)
    {
        this.domain = domain;
    }

    public void setDomainSize(int domainSize)
    {
        this.domainSize = domainSize;
    }

    public void incrementNumberOfNoGoodTuplesFiltered()
    {
        numberOfNoGoodTuplesFiltered += 1;
    }

    public int getNumberOfNoGoodTuplesFiltered()
    {
        return numberOfNoGoodTuplesFiltered;
    }

    public void setNumberOfTuples(long numberOfTuples)
    {
        this.numberOfTuples = numberOfTuples;
    }

    public long getNumberOfTuples()
    {
        return numberOfTuples;
    }

    public void setNoGoodListMapRepresentation(String noGoodListMapRepresentation)
    {
        this.noGoodListMapRepresentation = noGoodListMapRepresentation;
    }

    public String getNoGoodListMapRepresentation()
    {
        return noGoodListMapRepresentation;
    }

    @Override
    public void setFetchingTuplesTime(long fetchingTuplesTime)
    {
        this.fetchingTuplesTime = fetchingTuplesTime;
    }

    @Override
    public long getFetchingTuplesTime()
    {
        return fetchingTuplesTime;
    }

    @Override
    public long getNoGoodListProbingTime()
    {
        return noGoodListProbingTime;
    }

    @Override
    public void setNoGoodListProbingTime(long noGoodListProbingTime)
    {
        this.noGoodListProbingTime = noGoodListProbingTime;
    }

    @Override
    public void setNoGoodListSizeInBytes(long noGoodListSizeInBytes)
    {
        this.noGoodListSizeInBytes = noGoodListSizeInBytes;
    }

    @Override
    public long getNoGoodListSizeInBytes()
    {
        return this.noGoodListSizeInBytes;
    }

    @Override
    public String getRecordTupleSourceClazzName()
    {
        return recordTupleSourceProviderClazzName;
    }

    @Override
    public void setRecordTupleSourceClazzName(String recordTupleSourceClazzName)
    {
        this.recordTupleSourceProviderClazzName = recordTupleSourceClazzName;
    }

    @Override
    public void setPredicateEvaluationTime(long predicateEvaluationTime)
    {
        this.predicateEvaluationTime = predicateEvaluationTime;
    }

    @Override
    public long getPredicateEvaluationTime()
    {
        return predicateEvaluationTime;
    }

    @Override
    public long getNumberOfPassContextCalls()
    {
        return numberOfPassContextCalls;
    }

    @Override
    public void incrementNumberOfPassContextCalls()
    {
        numberOfPassContextCalls++;
    }
}
