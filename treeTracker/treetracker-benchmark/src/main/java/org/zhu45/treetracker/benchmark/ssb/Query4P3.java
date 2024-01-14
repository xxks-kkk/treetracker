package org.zhu45.treetracker.benchmark.ssb;

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
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getCustomerInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getDateInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getLineOrderInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getPartInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getSupplierInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query4P3
        extends Query
{
    public Query4P3(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode lineOrderNode = getLineOrderInt(SSBQueries.Q4P3);
        MultiwayJoinNode dateNode = getDateInt(SSBQueries.Q4P3);
        MultiwayJoinNode customerNode = getCustomerInt(SSBQueries.Q4P3);
        MultiwayJoinNode supplierNode = getSupplierInt(SSBQueries.Q4P3);
        MultiwayJoinNode partNode = getPartInt(SSBQueries.Q4P3);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(lineOrderNode, dateNode),
                asEdge(lineOrderNode, customerNode),
                asEdge(lineOrderNode, supplierNode),
                asEdge(lineOrderNode, partNode)), lineOrderNode);

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
