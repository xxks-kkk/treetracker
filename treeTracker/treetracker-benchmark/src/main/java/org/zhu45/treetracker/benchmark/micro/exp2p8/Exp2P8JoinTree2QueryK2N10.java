package org.zhu45.treetracker.benchmark.micro.exp2p8;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.getTableNode;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t0k2n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t1k2n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t2k2n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t3k2n10;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Exp2P8JoinTree2QueryK2N10
        extends Query
{
    public Exp2P8JoinTree2QueryK2N10(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode t0k2n10Node = getTableNode(t0k2n10);
        MultiwayJoinNode t1k2n10Node = getTableNode(t1k2n10);
        MultiwayJoinNode t2k2n10Node = getTableNode(t2k2n10);
        MultiwayJoinNode t3k2n10Node = getTableNode(t3k2n10);
        List<MultiwayJoinNode> traversalList = List.of(t0k2n10Node, t1k2n10Node, t2k2n10Node, t3k2n10Node);
        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(t0k2n10Node, Arrays.asList(asDirectedEdge(t0k2n10Node, t1k2n10Node), asDirectedEdge(t0k2n10Node, t2k2n10Node), asDirectedEdge(t0k2n10Node, t3k2n10Node)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(t0k2n10Node.getSchemaTableName(), t1k2n10Node.getSchemaTableName(), t2k2n10Node.getSchemaTableName(), t3k2n10Node.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
