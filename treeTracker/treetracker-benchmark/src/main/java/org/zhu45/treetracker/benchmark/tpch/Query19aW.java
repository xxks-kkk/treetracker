package org.zhu45.treetracker.benchmark.tpch;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getLineitem;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPart;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query19aW
        extends Query
{
    public Query19aW(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode partNode = getPart(TPCHQueries.Q19aW);
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q19aW);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(partNode, lineItemNode)), lineItemNode);

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
