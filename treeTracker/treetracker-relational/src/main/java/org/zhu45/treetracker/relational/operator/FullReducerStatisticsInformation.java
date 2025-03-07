package org.zhu45.treetracker.relational.operator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class FullReducerStatisticsInformation
        extends AbstractStatisticsInformation
{
    // number of tuples removed for each relation (Yannakakis's algorithm specific)

    private Map<String, Long> numberOfTuplesRemovedByFullReducer;
    // estimate cost of full reducer, which is also used as the cost of Yannakakis's algorithm.
    @JsonIgnore
    @Setter
    private Map<MultiwayJoinNode, List<Integer>> costMap;
    private long numberOfR1Assignments;
    // the summation of semijoin output size from full reducer (not counting the last returned null)
    private long summationOfSemijoinOutputSize;
    @Setter
    // After bottom-up pass, Yannakakis is early stopped due to one of the relations is completely empty
    private boolean earlyStoppedDueToBottomUpPass;
    @Setter
    // After top-down pass, Yannakakis is early stopped due to one of the relations is completely empty
    private boolean earlyStoppedDueToTopDownPass;
    @Setter
    // Pair.left() \leftsemijoin Pair.right() causes early stop of Yannakakis
    private Pair<MultiwayJoinNode, MultiwayJoinNode> semijoinCausingEarlyStop;
    // total runtime of executing full reducer
    private long fullReducerTime;
    // total number of hash table probe when executing semijoins in full reducer.
    private long numberOfHashTableProbe;
    // total number of tuples that we build hash table in full reducer.
    private long numberOfHashTableBuildTuples;
    // hash table build time
    private long hashTableBuildTime;

    public FullReducerStatisticsInformation()
    {
        numberOfTuplesRemovedByFullReducer = new HashMap<>();
    }

    public void updateNumberOfTuplesRemovedByFullReducer(String relationName, long tuplesRemoved)
    {
        if (numberOfTuplesRemovedByFullReducer.containsKey(relationName)) {
            numberOfTuplesRemovedByFullReducer.put(relationName, numberOfTuplesRemovedByFullReducer.get(relationName) + tuplesRemoved);
        }
        else {
            numberOfTuplesRemovedByFullReducer.put(relationName, tuplesRemoved);
        }
    }

    public void updateNumberOfR1Assignments(long numberOfR1Assignments)
    {
        this.numberOfR1Assignments += numberOfR1Assignments;
    }

    public void updateSummationOfSemijoinOutputSize(long summationOfSemijoinOutputSize)
    {
        this.summationOfSemijoinOutputSize += summationOfSemijoinOutputSize;
    }

    public void setFullReducerTime(long fullReducerTime)
    {
        this.fullReducerTime = fullReducerTime;
    }

    @Override
    public void updateNumberOfHashTableProbe(long numberOfHashTableProbe)
    {
        this.numberOfHashTableProbe += numberOfHashTableProbe;
    }

    @Override
    public void updateNumberOfHashTableBuildTuples(long numberOfHashTableBuildTuples)
    {
        this.numberOfHashTableBuildTuples += numberOfHashTableBuildTuples;
    }

    @Override
    public long getNumberOfHashTableBuildTuples()
    {
        return this.numberOfHashTableBuildTuples;
    }

    @Override
    public void setHashTableBuildTime(long hashTableBuildTime)
    {
        this.hashTableBuildTime += hashTableBuildTime;
    }
}
