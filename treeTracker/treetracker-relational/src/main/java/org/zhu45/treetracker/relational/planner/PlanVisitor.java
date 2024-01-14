package org.zhu45.treetracker.relational.planner;

import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

public abstract class PlanVisitor<R, C>
{
    /**
     * The default behavior to perform when visiting a PlanNode
     */
    public abstract R visitPlan(PlanNode node, C context);

    public R visitJoin(JoinNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitTable(TableNode node, C context)
    {
        return visitPlan(node, context);
    }

    public R visitFullReducer(FullReducerNode node, C context)
    {
        return visitPlan(node, context);
    }
}
