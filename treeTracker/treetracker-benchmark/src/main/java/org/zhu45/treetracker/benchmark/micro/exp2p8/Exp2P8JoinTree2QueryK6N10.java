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
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t0k6n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t1k6n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t2k6n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t3k6n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t4k6n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t5k6n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t6k6n10;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.t7k6n10;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Exp2P8JoinTree2QueryK6N10
        extends Query
{
    public Exp2P8JoinTree2QueryK6N10(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode t0k6n10Node = getTableNode(t0k6n10);
        MultiwayJoinNode t1k6n10Node = getTableNode(t1k6n10);
        MultiwayJoinNode t2k6n10Node = getTableNode(t2k6n10);
        MultiwayJoinNode t3k6n10Node = getTableNode(t3k6n10);
        MultiwayJoinNode t4k6n10Node = getTableNode(t4k6n10);
        MultiwayJoinNode t5k6n10Node = getTableNode(t5k6n10);
        MultiwayJoinNode t6k6n10Node = getTableNode(t6k6n10);
        MultiwayJoinNode t7k6n10Node = getTableNode(t7k6n10);
        List<MultiwayJoinNode> traversalList = List.of(t0k6n10Node, t1k6n10Node, t2k6n10Node, t3k6n10Node, t4k6n10Node, t5k6n10Node, t6k6n10Node, t7k6n10Node);
        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(t0k6n10Node, Arrays.asList(asDirectedEdge(t0k6n10Node, t1k6n10Node), asDirectedEdge(t0k6n10Node, t2k6n10Node), asDirectedEdge(t0k6n10Node, t3k6n10Node), asDirectedEdge(t0k6n10Node, t4k6n10Node), asDirectedEdge(t0k6n10Node, t5k6n10Node), asDirectedEdge(t0k6n10Node, t6k6n10Node), asDirectedEdge(t0k6n10Node, t7k6n10Node)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(t0k6n10Node.getSchemaTableName(), t1k6n10Node.getSchemaTableName(), t2k6n10Node.getSchemaTableName(), t3k6n10Node.getSchemaTableName(), t4k6n10Node.getSchemaTableName(), t5k6n10Node.getSchemaTableName(), t6k6n10Node.getSchemaTableName(), t7k6n10Node.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
