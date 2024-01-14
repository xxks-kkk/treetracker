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
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t0k5n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t1k5n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t2k5n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t3k5n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t4k5n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t5k5n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t6k5n10;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Exp2P8JoinTree1QueryK5N10
        extends Query
{
    public Exp2P8JoinTree1QueryK5N10(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode t0k5n10Node = getTableNode(t0k5n10);
        MultiwayJoinNode t1k5n10Node = getTableNode(t1k5n10);
        MultiwayJoinNode t2k5n10Node = getTableNode(t2k5n10);
        MultiwayJoinNode t3k5n10Node = getTableNode(t3k5n10);
        MultiwayJoinNode t4k5n10Node = getTableNode(t4k5n10);
        MultiwayJoinNode t5k5n10Node = getTableNode(t5k5n10);
        MultiwayJoinNode t6k5n10Node = getTableNode(t6k5n10);
        List<MultiwayJoinNode> traversalList = List.of(t0k5n10Node, t1k5n10Node, t2k5n10Node, t3k5n10Node, t4k5n10Node, t5k5n10Node, t6k5n10Node);
        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(t0k5n10Node, Arrays.asList(asDirectedEdge(t0k5n10Node, t1k5n10Node), asDirectedEdge(t1k5n10Node, t2k5n10Node), asDirectedEdge(t2k5n10Node, t3k5n10Node), asDirectedEdge(t3k5n10Node, t4k5n10Node), asDirectedEdge(t4k5n10Node, t5k5n10Node), asDirectedEdge(t5k5n10Node, t6k5n10Node)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(t0k5n10Node.getSchemaTableName(), t1k5n10Node.getSchemaTableName(), t2k5n10Node.getSchemaTableName(), t3k5n10Node.getSchemaTableName(), t4k5n10Node.getSchemaTableName(), t5k5n10Node.getSchemaTableName(), t6k5n10Node.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
