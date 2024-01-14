package org.zhu45.treetracker.relational.planner;

import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.LinkedList;
import java.util.List;

public class TestingPlanVisitor
        extends PlanVisitor<Boolean, LinkedList<OptType>>
{
    @Override
    public Boolean visitPlan(PlanNode node, LinkedList<OptType> context) {
        if (node == null) return true;
        List<PlanNode> children = node.getSources();
        for (PlanNode child : children) {
            if (child != null) {
                boolean visit = visitPlan(child, context);
                if (!visit) return false;
            }
        }
        return node.accept(this, context);
    }

    @Override
    public Boolean visitJoin(JoinNode node, LinkedList<OptType> context) {
        return visitNode(node, context);
    }

    @Override
    public Boolean visitTable(TableNode node, LinkedList<OptType> context) {
        return visitNode(node, context);
    }

    private Boolean visitNode(PlanNode node, LinkedList<OptType> context) {
        OptType expectedType = context.getFirst();
        context.removeFirst();
        return expectedType.equals(node.getNodeType());
    }
}
