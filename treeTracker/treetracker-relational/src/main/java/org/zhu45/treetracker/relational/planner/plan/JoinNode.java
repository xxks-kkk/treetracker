package org.zhu45.treetracker.relational.planner.plan;

import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeId;
import org.zhu45.treetracker.relational.planner.PlanVisitor;

import java.util.Arrays;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JoinNode
        extends PlanNode
{
    private PlanNode left;
    private PlanNode right;
    private static final OptType NODE_PLAN_NODE_TYPE = OptType.join;

    public JoinNode(PlanNodeId id, PlanNode left, PlanNode right, Side side)
    {
        super(id, side);

        requireNonNull(left, "left is null");
        requireNonNull(right, "right is null");

        this.left = left;
        this.right = right;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Arrays.asList(this.left, this.right);
    }

    public PlanNode getLeft()
    {
        return left;
    }

    public PlanNode getRight()
    {
        return right;
    }

    public void setRight(PlanNode right)
    {
        this.right = right;
    }

    public void setLeft(PlanNode left)
    {
        this.left = left;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitJoin(this, context);
    }

    @Override
    public OptType getNodeType()
    {
        return NODE_PLAN_NODE_TYPE;
    }

    @Override
    public String toString()
    {
        return "(" + left.toString() + " JOIN " + right.toString() + ")";
    }
}
