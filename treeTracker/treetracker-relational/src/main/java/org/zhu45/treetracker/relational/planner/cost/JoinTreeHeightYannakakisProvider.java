package org.zhu45.treetracker.relational.planner.cost;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * In addition to the JoinTreeHeightProvider, this class generates the semijoin ordering based
 * on the input JoinOrdering.
 */
public class JoinTreeHeightYannakakisProvider
        extends JoinTreeHeightProvider
{
    @Override
    public JoinTreeCostReturn getCost(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree, PlanBuildContext context)
    {
        JoinTreeCostReturn costReturn = super.getCost(ordering, joinTree, context);
        double depth = costReturn.getCost();
        SemiJoinOrdering semiJoinOrdering = decideSemiJoinOrdering(ordering, joinTree);
        return JoinTreeCostReturn.builder(depth)
                .setSemijoinOrdering(semiJoinOrdering)
                .build();
    }

    private SemiJoinOrdering decideSemiJoinOrdering(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree)
    {
        int sizeOfOrdering = joinTree.getTraversalList().size();
        SemiJoinOrdering semiJoinOrdering = new SemiJoinOrdering(new ArrayList<>(), joinTree);
        for (int i = 0; i < sizeOfOrdering; ++i) {
            MultiwayJoinNode joinNode = joinTree.getTraversalList().get(i);
            switch (joinNode.getNodeType()) {
                case Leaf:
                    break;
                case Root:
                case Internal:
                    SemiJoinOrdering order = costBasedOnDefaultSemijoinOrdering(joinNode, joinTree, ordering);
                    semiJoinOrdering.insertAtHead(order);
                    break;
                default:
                    throw new RuntimeException("joinNode has NodeType: " + joinNode.getNodeType());
            }
        }
        return semiJoinOrdering;
    }

    private SemiJoinOrdering costBasedOnDefaultSemijoinOrdering(MultiwayJoinNode joinNode,
                                                                MultiwayJoinOrderedGraph joinTree,
                                                                JoinOrdering joinOrdering)
    {
        checkArgument(joinNode.getNodeType() == NodeType.Internal ||
                        joinNode.getNodeType() == NodeType.Root,
                joinNode + "is neither internal nor root");
        List<MultiwayJoinNode> children = joinTree.getChildren().get(joinNode);
        /**
         * We create a min heap and use the index of the relation in the join ordering to decide the order of
         * semijoins. For example, for JOB Q10a, we have the following join order
         * [cast_info, roleType, title, mc, cn, charName, ct] and the join tree:
         * <p>
         * imdb.q10a_cast_info(person_id,movie_id,person_role_id,role_id)
         * |imdb.q10a_role_type(role_id)
         * |imdb.q10a_title(movie_id,kind_id)
         * |imdb_int.movie_companies(movie_id,company_id,company_type_id)
         * ||imdb.q10a_company_name(company_id)
         * ||imdb_int.company_type(company_type_id)
         * |imdb_int.char_name(person_role_id)
         * <p>
         * For mc, it has two children: cn and ct. To decide semijoin order, we note cn in the join order
         * has index 4 and ct in the join order has index 6. Thus, the semijoin order for mc is
         * mc \leftsemijoin cn, mc \leftsemijoin ct.
         */
        PriorityQueue<Pair<Integer, MultiwayJoinNode>> pq =
                new PriorityQueue<>(children.size(),
                        Comparator.comparingInt(Pair::getKey));
        for (MultiwayJoinNode child : children) {
            pq.add(Pair.of(joinOrdering.getSchemaTableNameList().indexOf(child.getSchemaTableName()), child));
        }
        List<Pair<MultiwayJoinNode, MultiwayJoinNode>> semijoins = new ArrayList<>();
        while (!pq.isEmpty()) {
            semijoins.add(Pair.of(joinNode, pq.poll().getRight()));
        }
        return new SemiJoinOrdering(semijoins, joinTree);
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
