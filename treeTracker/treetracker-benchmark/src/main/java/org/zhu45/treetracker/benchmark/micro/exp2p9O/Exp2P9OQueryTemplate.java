package org.zhu45.treetracker.benchmark.micro.exp2p9O;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9ODatabase.exp2P9R;
import static org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9ODatabase.exp2P9T;
import static org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9ODatabase.exp2P9U;
import static org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9ODatabase.exp2P9V;
import static org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9ODatabase.exp2P9W;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

/**
 * Query for Exp2.9: Impact of Removing Dangling Tuples from R_{inner}. This is actually
 * the query we write in the experiment document.
 * <p>
 * R(a,c)
 * |U(c,e)
 * |V(c,d)
 * ||T(d,e)
 * ||W(d,f)
 */
public abstract class Exp2P9OQueryTemplate
        extends Query
{
    public Exp2P9OQueryTemplate(JoinFragmentContext context)
    {
        super(context);
    }

    protected Pair<Plan, List<Operator>> constructQuery(int semijoinmodratio)
    {
        MultiwayJoinNode exp2P9RNode = Exp2P9ODatabase.getTableNode(exp2P9R, semijoinmodratio);
        MultiwayJoinNode exp2P9UNode = Exp2P9ODatabase.getTableNode(exp2P9U, semijoinmodratio);
        MultiwayJoinNode exp2P9VNode = Exp2P9ODatabase.getTableNode(exp2P9V, semijoinmodratio);
        MultiwayJoinNode exp2P9TNode = Exp2P9ODatabase.getTableNode(exp2P9T, semijoinmodratio);
        MultiwayJoinNode exp2P9WNode = Exp2P9ODatabase.getTableNode(exp2P9W, semijoinmodratio);

        List<MultiwayJoinNode> traversalList = List.of(exp2P9RNode, exp2P9UNode, exp2P9VNode, exp2P9TNode, exp2P9WNode);

        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(exp2P9RNode, Arrays.asList(
                asDirectedEdge(exp2P9RNode, exp2P9UNode),
                asDirectedEdge(exp2P9UNode, exp2P9VNode),
                asDirectedEdge(exp2P9VNode, exp2P9TNode),
                asDirectedEdge(exp2P9VNode, exp2P9WNode)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(exp2P9RNode.getSchemaTableName(),
                exp2P9UNode.getSchemaTableName(),
                exp2P9VNode.getSchemaTableName(),
                exp2P9TNode.getSchemaTableName(),
                exp2P9WNode.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
