package org.zhu45.treetracker.relational.statistics;

import lombok.Getter;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeIdAllocator;
import org.zhu45.treetracker.relational.planner.PlanVisitor;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.ArrayList;
import java.util.List;

public class CostModelContext
{
    @Getter private final MultiwayJoinOrderedGraph orderedGraph;
    @Getter private final List<MultiwayJoinNode> childrenOfRoot;
    @Getter private final JdbcClient jdbcClient;
    @Getter private final List<MultiwayJoinNode> multiwayJoinNodesInPlan;
    @Getter private final PlanNode planRoot;
    @Getter private final PlanNodeIdAllocator planNodeIdAllocator;
    @Getter private final JoinFragmentType query;

    private CostModelContext(Builder builder)
    {
        this.orderedGraph = builder.orderedGraph;
        this.childrenOfRoot = builder.childrenOfRoot;
        this.jdbcClient = builder.jdbcClient;
        this.planRoot = builder.planRoot;
        this.multiwayJoinNodesInPlan = builder.multiwayJoinNodesInPlan;
        this.planNodeIdAllocator = builder.planNodeIdAllocator;
        this.query = builder.query;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private MultiwayJoinOrderedGraph orderedGraph;
        private List<MultiwayJoinNode> childrenOfRoot;
        private JdbcClient jdbcClient;
        private List<MultiwayJoinNode> multiwayJoinNodesInPlan;
        private PlanNode planRoot;
        private PlanNodeIdAllocator planNodeIdAllocator;
        private JoinFragmentType query;

        public Builder()
        {
        }

        public Builder setOrderedGraph(MultiwayJoinOrderedGraph orderedGraph)
        {
            this.orderedGraph = orderedGraph;
            MultiwayJoinNode root = (MultiwayJoinNode) orderedGraph.getRoot();
            childrenOfRoot = orderedGraph.getChildren().get(root);
            return this;
        }

        public Builder setJdbcClient(JdbcClient jdbcClient)
        {
            this.jdbcClient = jdbcClient;
            return this;
        }

        public Builder setPlanNodeIdAllocator(PlanNodeIdAllocator allocator)
        {
            this.planNodeIdAllocator = allocator;
            return this;
        }

        public Builder setJoinFragmentType(JoinFragmentType query)
        {
            this.query = query;
            this.planRoot = query.getPlan().getRoot();
            GatherMultiwayJoinNode gatherMultiwayJoinNode = GatherMultiwayJoinNode.getGatherMultiwayJoinNode();
            gatherMultiwayJoinNode.visitPlan(planRoot, null);
            multiwayJoinNodesInPlan = gatherMultiwayJoinNode.getMultiwayJoinNodes();
            return this;
        }

        public CostModelContext build()
        {
            return new CostModelContext(this);
        }
    }

    public static class GatherMultiwayJoinNode
            extends PlanVisitor<Void, Void>
    {
        @Getter
        List<MultiwayJoinNode> multiwayJoinNodes;

        public GatherMultiwayJoinNode()
        {
            multiwayJoinNodes = new ArrayList<>();
        }

        public static GatherMultiwayJoinNode getGatherMultiwayJoinNode()
        {
            return new GatherMultiwayJoinNode();
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            if (node == null) {
                return null;
            }
            List<PlanNode> children = node.getSources();
            for (PlanNode child : children) {
                if (child != null) {
                    visitPlan(child, context);
                }
            }
            return node.accept(this, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            return null;
        }

        @Override
        public Void visitTable(TableNode node, Void context)
        {
            return visitNode(node, context);
        }

        @Override
        public Void visitFullReducer(FullReducerNode node, Void context)
        {
            return null;
        }

        private Void visitNode(PlanNode node, Void context)
        {
            TableNode tableNode = (TableNode) node;
            multiwayJoinNodes.add(tableNode.getOperator().getMultiwayJoinNode());
            return null;
        }
    }
}
