package org.zhu45.treetracker.relational.statistics;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;

import java.util.HashMap;

public interface CostModelStatisticsData
{
    String getId();
    void setContext(CostModelContext context);
    HashMap<MultiwayJoinNode, Integer> getRelationSizes();
    HashMap<MultiwayJoinNode, Float> getSemijoinSelectivities();
    HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Float>> getAntijoinSelectivities();
    HashMap<MultiwayJoinNode, Long> getFullyReducedRelationSizes();
    HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> getDSR();
}
