package org.zhu45.treetracker.relational.planner.printer;

import org.zhu45.treetracker.relational.operator.FullReducerOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeId;
import org.zhu45.treetracker.relational.planner.PlanVisitor;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Print logical plan and physical plan in various formats
 */
public class PlanPrinter
{
    private final PlanRepresentation representation;

    public PlanPrinter(PlanNode planRoot)
    {
        requireNonNull(planRoot, "planRoot is null");

        this.representation = new PlanRepresentation(planRoot);
        Visitor visitor = new Visitor();
        // We use pre-order traversal for visitor pattern to print out plan
        planRoot.accept(visitor, null);
    }

    public String toText(int level)
    {
        return new TextRenderer(level).render(representation);
    }

    private class Visitor
            extends PlanVisitor<Void, Void>
    {
        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            NodeRepresentation nodeOutput =
                    addNode(node, "Join", String.format("%s join %s", node.getLeft().getId(), node.getRight().getId()));
            if (node.getOperator() != null) {
                nodeOutput.appendDetailsLine("operator = %s", node.getOperator().getOperatorName());
            }
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);
            return null;
        }

        @Override
        public Void visitTable(TableNode node, Void context)
        {
            NodeRepresentation nodeOutput =
                    addNode(node, "Table", node.getSchemaTableName().toString());
            if (node.getOperator() != null) {
                nodeOutput.appendDetailsLine("operator = %s", node.getOperator().getOperatorName());
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitFullReducer(FullReducerNode node, Void context)
        {
            NodeRepresentation nodeOutput =
                    addNode(node, "Full Reducer");
            if (node.getOperator() != null) {
                FullReducerOperator operator = (FullReducerOperator) node.getOperator();
                nodeOutput.appendDetailsLine("operator = %s", operator.getOperatorName());
                nodeOutput.appendDetailsLine("Bottom-up semijoins: ");
                List<Plan> bottomUpSemijoins = operator.getBottomUpSemijoins();
                bottomUpSemijoins.forEach(plan -> {
                    PlanPrinter printer = new PlanPrinter(plan.getRoot());
                    nodeOutput.appendDetails(printer.toText(0));
                });
                nodeOutput.appendDetailsLine("Top-down semijoins: ");
                List<Plan> topDownSemijoins = operator.getTopDownSemijoins();
                topDownSemijoins.forEach(plan -> {
                    PlanPrinter printer = new PlanPrinter(plan.getRoot());
                    nodeOutput.appendDetails(printer.toText(0));
                });
            }
            node.getSink().accept(this, context);
            return null;
        }

        private Void processChildren(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }
            return null;
        }

        public NodeRepresentation addNode(PlanNode node, String name)
        {
            return addNode(node, name, "");
        }

        public NodeRepresentation addNode(PlanNode node, String name, String identifier)
        {
            return addNode(node, name, identifier, node.getSources());
        }

        public NodeRepresentation addNode(PlanNode rootNode, String name, String identifier, List<PlanNode> children)
        {
            List<PlanNodeId> childrenIds = children.stream().map(PlanNode::getId).collect(toImmutableList());
            NodeRepresentation nodeOutput = new NodeRepresentation(rootNode.getId(), name, identifier, childrenIds);
            representation.addNode(nodeOutput);

            return nodeOutput;
        }
    }
}
