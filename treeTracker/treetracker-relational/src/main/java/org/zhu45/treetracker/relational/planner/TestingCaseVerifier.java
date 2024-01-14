package org.zhu45.treetracker.relational.planner;

import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.LinkedList;
import java.util.List;

/**
 * Verify if the manual test case created as expected.
 */
public class TestingCaseVerifier
        extends PlanVisitor<Boolean, LinkedList<SchemaTableName>>
{
    @Override
    public Boolean visitPlan(PlanNode node, LinkedList<SchemaTableName> context)
    {
        if (node == null) {
            return true;
        }
        List<PlanNode> children = node.getSources();
        for (PlanNode child : children) {
            if (child != null) {
                boolean visit = visitPlan(child, context);
                if (!visit) {
                    return false;
                }
            }
        }
        return node.accept(this, context);
    }

    @Override
    public Boolean visitJoin(JoinNode node, LinkedList<SchemaTableName> context)
    {
        return true;
    }

    @Override
    public Boolean visitTable(TableNode node, LinkedList<SchemaTableName> context)
    {
        return visitNode(node, context);
    }

    @Override
    public Boolean visitFullReducer(FullReducerNode node, LinkedList<SchemaTableName> context)
    {
        return true;
    }

    private Boolean visitNode(PlanNode node, LinkedList<SchemaTableName> context)
    {
        SchemaTableName expectedSchemaTableName = context.getFirst();
        context.removeFirst();
        TableNode tableNode = (TableNode) node;
        return expectedSchemaTableName.equals(tableNode.getSchemaTableName());
    }
}
