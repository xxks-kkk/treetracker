package org.zhu45.treetracker.relational.planner.cost;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

/**
 * Evaluating the cost of a join tree
 */
public interface JoinTreeCostProvider
{
    JoinTreeCostReturn getCost(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree, PlanBuildContext context);

    boolean isUseTrueCard();

    default JoinTreeCostStatistics getStatistics()
    {
        throw new UnsupportedOperationException();
    }
}
