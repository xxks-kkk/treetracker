package org.zhu45.treetracker.relational;

import lombok.Builder;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.PlanNode;

import java.util.ArrayList;
import java.util.List;

import static org.zhu45.treetracker.relational.planner.PlanBuilder.findRootOperatorPlanNode;

/**
 * This class is used to characterize a specific Operator or PlanNode so that we can find the Operator or PlanNode
 * based on the provided specification.
 */
@Builder
public class OperatorSpecification
{
    OptType optType;
    String relationName;
    String virtualRelationName;
    List<OperatorSpecification> children;

    public static Operator findTargetOperator(PlanNode root, OperatorSpecification operatorSpecification)
    {
        if (isMatch(root, operatorSpecification)) {
            return root.getOperator();
        }
        for (PlanNode child : root.getSources()) {
            Operator find = findTargetOperator(findRootOperatorPlanNode(child), operatorSpecification);
            if (find != null) {
                return find;
            }
        }
        return null;
    }

    public static boolean isMatch(PlanNode candidateNode, OperatorSpecification targetNode)
    {
        boolean typeMatch = candidateNode.getNodeType() == targetNode.optType;
        boolean nameMatch = true;
        boolean childrenMatch = true;
        if (targetNode.relationName != null) {
            if (candidateNode.getSchemaTableName() != null) {
                nameMatch = candidateNode.getSchemaTableName().getTableName().equals(targetNode.relationName);
            }
            else {
                nameMatch = false;
            }
        }
        if (targetNode.virtualRelationName != null) {
            if (candidateNode.getVirtualSchemaTableName() != null) {
                nameMatch = candidateNode.getVirtualSchemaTableName().getTableName().equals(targetNode.virtualRelationName);
            }
            else {
                nameMatch = false;
            }
        }
        if (targetNode.children != null) {
            if (candidateNode.getSources().size() != targetNode.children.size()) {
                childrenMatch = false;
            }
            else {
                for (int i = 0; i < candidateNode.getSources().size(); i++) {
                    childrenMatch = childrenMatch && isMatch(candidateNode.getSources().get(0), targetNode.children.get(0));
                }
            }
        }
        return childrenMatch && typeMatch && nameMatch;
    }

    public static OperatorSpecification intoSpecification(PlanNode node)
    {
        // The specification only consists of the node and its children. We stick
        // to this for now to see if we run into any issue during the load planStatistics.
        // If we do, we can increase the targetLevel.
        return intoSpecificationHelper(node, 0, 1);
    }

    private static OperatorSpecification intoSpecificationHelper(PlanNode node, Integer currentLevel, Integer targetLevel)
    {
        List<OperatorSpecification> children = new ArrayList<>();
        if (currentLevel < targetLevel) {
            for (PlanNode child : node.getSources()) {
                children.add(intoSpecificationHelper(child, currentLevel + 1, targetLevel));
            }
        }
        OperatorSpecificationBuilder builder = new OperatorSpecificationBuilder();
        switch (node.getNodeType()) {
            case table:
                builder.optType(OptType.table)
                        .relationName(node.getSchemaTableName().getTableName());
                break;
            case materialize:
            case fullReducer:
            case hash:
            case gather:
            case aggregate:
            case sort:
            case gather_merge:
                builder.optType(node.getNodeType());
                break;
            case join:
                builder.optType(OptType.join);
                if (node.getSchemaTableName() != null) {
                    builder.relationName(node.getSchemaTableName().getTableName());
                }
                if (node.getVirtualSchemaTableName() != null) {
                    builder.virtualRelationName(node.getVirtualSchemaTableName().getTableName());
                }
                break;
        }
        return builder
                .children(children)
                .build();
    }
}
