package org.zhu45.treetracker.relational.planner.cost;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

/**
 * Returns the height of join tree as cost, which helps to implement the heuristic that
 * use the join tree with as the smallest height as possible.
 */
public class JoinTreeHeightProvider
        implements JoinTreeCostProvider
{
    @Override
    public JoinTreeCostReturn getCost(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree, PlanBuildContext context)
    {
        return JoinTreeCostReturn.builder(joinTree.getDepth().size()).build();
    }

    @Override
    public boolean isUseTrueCard()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JoinTreeCostStatistics getStatistics()
    {
        return JoinTreeCostStatistics.builder(this.getClass())
                .build();
    }
}
