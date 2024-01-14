package org.zhu45.treetracker.benchmark.tpch;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.QueryGraphEdge;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPart;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPartsupp;

public class Query16W
        extends Query
{
    public Query16W(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode partSuppNode = getPartsupp(TPCHQueries.Q16W);
        MultiwayJoinNode partNode = getPart(TPCHQueries.Q16W);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(partSuppNode, partNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, partNode);

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
