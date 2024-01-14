package org.zhu45.treetracker.relational.operator;

public class TupleBasedJoinStatisticsInformation
        extends AbstractStatisticsInformation
{
    // number of times that R1 get assigned
    private long numberOfR1Assignments;
    // time spent on join (approx. to intermediate result generation time)
    private long joinTime;
    // number of dangling tuples produced
    private long numberOfDanglingTuples;

    public void incrementNumberOfR1Assignments()
    {
        numberOfR1Assignments += 1;
    }

    public long getNumberOfR1Assignments()
    {
        return numberOfR1Assignments;
    }

    @Override
    public void incrementNumberOfDanglingTuples()
    {
        numberOfDanglingTuples += 1;
    }

    @Override
    public long getNumberOfDanglingTuples()
    {
        return numberOfDanglingTuples;
    }

    @Override
    public void updateJoinTime(long joinTime)
    {
        this.joinTime += joinTime;
    }

    @Override
    public long getJoinTime()
    {
        return joinTime;
    }
}
