package org.zhu45.treetracker.relational.operator;

import lombok.Getter;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;

import java.util.HashMap;

public class TTJStatisticsInformation
        extends TupleBasedHashJoinStatisticsInformation
{
    // number of no-good tuples (TTJ specific)
    @Getter
    private int numberOfNoGoodTuples;
    // key: relation, value: the number of no-good tuples.
    // The stats keeps track of the number of no-good tuples contributed by each child relation of the current operator's R2.
    // This implements cost model 3.
    @Getter
    private HashMap<MultiwayJoinNode, Integer> nodeToNoGoodTuples = new HashMap<>();
    // number of map keys in noGoodListMap;
    @Getter
    private int noGoodListMapKeyNum;
    // time spent in PassContext() (only count on the logic around removing tuples from H)
    @Getter
    private long passContextWorkTime;
    // time spent on removing tuples from internal hash table (similar to passContextWorkTime but
    // this metric includes the PassContext() call time itself (i.e., we start to track time since the initiation of
    // PassContext() call
    @Getter
    private long deleteDanglingTupleFromHTime;
    // time spent on building no-good list.
    @Getter
    private long buildNoGoodListTime;
    // time spent on construct no-good list. This is difference from buildNoGoodListTime because
    // this only measures the time spent in R_k table scan passContext() call whereas buildNoGoodListTime starts
    // the timer when PassContext() is initiated at some R_k's child relation
    @Getter
    private long noGoodListConstructTime;
    // number of times PassContext() get called
    @Getter
    private long numberOfPassContextCalls;
    // number of times PassContext() is called, i.e., initiated due to probing failure from hash table
    @Getter
    private long numberOfInitPassContextCalls;
    @Getter private long numberOfDeletionPropagationTriggered;

    public void setNumberOfNoGoodTuples(int numberOfNoGoodTuples)
    {
        this.numberOfNoGoodTuples = numberOfNoGoodTuples;
    }

    public void incrementNumberOfNoGoodTuples()
    {
        numberOfNoGoodTuples += 1;
    }

    public void incrementNodeToNoGoodTuples(MultiwayJoinNode node)
    {
        if (!nodeToNoGoodTuples.containsKey(node)) {
            nodeToNoGoodTuples.put(node, 0);
        }
        nodeToNoGoodTuples.put(node, nodeToNoGoodTuples.get(node) + 1);
    }

    public void setNoGoodListMapKeyNum(int noGoodListMapKeyNum)
    {
        this.noGoodListMapKeyNum = noGoodListMapKeyNum;
    }

    @Override
    public void updatePassContextWorkTime(long passContextWorkTime)
    {
        this.passContextWorkTime += passContextWorkTime;
    }

    @Override
    public void updateDeleteDanglingTupleFromHTime(long deleteDanglingTupleFromHTime)
    {
        this.deleteDanglingTupleFromHTime += deleteDanglingTupleFromHTime;
    }

    @Override
    public void updateBuildNoGoodListTime(long buildNoGoodListTime)
    {
        this.buildNoGoodListTime += buildNoGoodListTime;
    }

    public long getNoGoodListConstructTime()
    {
        return noGoodListConstructTime;
    }

    @Override
    public void setNoGoodListConstructTime(long noGoodListConstructTime)
    {
        this.noGoodListConstructTime = noGoodListConstructTime;
    }

    @Override
    public void incrementNumberOfPassContextCalls()
    {
        numberOfPassContextCalls++;
    }

    @Override
    public void incrementNumberOfInitPassContextCalls()
    {
        numberOfInitPassContextCalls++;
    }

    @Override
    public void incrementNumberOfDeletionPropagationTriggered()
    {
        numberOfDeletionPropagationTriggered++;
    }
}
