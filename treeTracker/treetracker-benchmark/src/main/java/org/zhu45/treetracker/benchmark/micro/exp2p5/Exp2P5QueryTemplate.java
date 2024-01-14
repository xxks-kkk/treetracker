package org.zhu45.treetracker.benchmark.micro.exp2p5;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.benchmark.micro.exp2p5.Exp2P5Database.exp2P5R;
import static org.zhu45.treetracker.benchmark.micro.exp2p5.Exp2P5Database.exp2P5S;
import static org.zhu45.treetracker.benchmark.micro.exp2p5.Exp2P5Database.exp2P5T;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

/**
 * query for Exp2.5: Impact of No-good Tuples at R_k
 * <p>
 * exp2p5.t(a,b)
 * |exp2p5.r(a)
 * |exp2p5.s(b)
 */
public abstract class Exp2P5QueryTemplate
        extends Query
{
    public Exp2P5QueryTemplate(JoinFragmentContext context)
    {
        super(context);
    }

    protected Pair<Plan, List<Operator>> constructQuery(int dupratio)
    {
        MultiwayJoinNode exp2P5TNode = Exp2P5Database.getTableNode(exp2P5T, dupratio);
        MultiwayJoinNode exp2P5RNode = Exp2P5Database.getTableNode(exp2P5R, dupratio);
        MultiwayJoinNode exp2P5SNode = Exp2P5Database.getTableNode(exp2P5S, dupratio);

        List<MultiwayJoinNode> traversalList = List.of(exp2P5TNode, exp2P5RNode, exp2P5SNode);

        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(exp2P5TNode, Arrays.asList(
                asDirectedEdge(exp2P5TNode, exp2P5RNode),
                asDirectedEdge(exp2P5TNode, exp2P5SNode)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(exp2P5TNode.getSchemaTableName(),
                exp2P5RNode.getSchemaTableName(),
                exp2P5SNode.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
