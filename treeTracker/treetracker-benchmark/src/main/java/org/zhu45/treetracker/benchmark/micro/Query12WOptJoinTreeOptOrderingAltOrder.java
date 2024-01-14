package org.zhu45.treetracker.benchmark.micro;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueries;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getLineitem;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getOrders;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Query12WOptJoinTreeOptOrderingAltOrder
        extends Query
{
    public Query12WOptJoinTreeOptOrderingAltOrder(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode ordersNode = getOrders(TPCHQueries.Q12W);
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q12W);

        List<MultiwayJoinNode> traversalList = List.of(ordersNode, lineItemNode);

        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(lineItemNode, Arrays.asList(
                asDirectedEdge(ordersNode, lineItemNode)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(ordersNode.getSchemaTableName(),
                lineItemNode.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
