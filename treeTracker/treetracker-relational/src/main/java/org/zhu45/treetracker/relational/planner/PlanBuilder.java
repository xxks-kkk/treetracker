package org.zhu45.treetracker.relational.planner;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.rule.Rule;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.logging.log4j.core.util.Assert.requireNonEmpty;

/**
 * PlanBuilder takes a list of table names or a query graph
 * and builds a logical plan out of it. The logical plan
 * is a left-deep join tree. Since we're only interested in
 * trying out different binary operator implementation and some optimization
 * technique on physical plan execution, we don't need fancy stuff like walk through AST
 * to perform semantic analysis and construct query plan (e.g., Presto).
 */
public class PlanBuilder
{
    private final List<SchemaTableName> schemaTableNameList;
    private final PlanNodeIdAllocator idAllocator;
    private final MultiwayJoinOrderedGraph orderedGraph;
    private final PlanBuildContext planBuildContext;
    private final List<Rule> rules;

    public PlanBuilder(List<SchemaTableName> schemaTableNameList, PlanBuildContext context)
    {
        this.schemaTableNameList = requireNonNull(schemaTableNameList, "schemaTableNameList is null");
        requireNonEmpty(schemaTableNameList, "schemaTableNameList is empty");
        this.planBuildContext = requireNonNull(context, "context is null");
        this.idAllocator = requireNonNull(context.getPlanNodeIdAllocator(), "idAllocator is null");
        this.rules = requireNonNull(context.getRules(), "rules is null");
        this.orderedGraph = context.getOrderedGraph();
    }

    public PlanBuilder(PlanBuildContext context)
    {
        this.planBuildContext = requireNonNull(context, "context is null");
        this.idAllocator = requireNonNull(context.getPlanNodeIdAllocator(), "idAllocator is null");
        this.orderedGraph = requireNonNull(context.getOrderedGraph(), "orderedGraph is null");
        this.rules = requireNonNull(context.getRules(), "rules is null");
        this.schemaTableNameList = orderedGraph.getTraversalList().stream().map(MultiwayJoinNode::getSchemaTableName).collect(Collectors.toList());
    }

    /**
     * Creates a logical plan that has left-deep join tree structure
     *
     * @return a logical plan
     */
    public Plan build()
    {
        Plan plan;
        if (orderedGraph == null) {
            plan = new Plan(buildHelper());
        }
        else {
            plan = new Plan(buildWithOrderedGraph());
        }
        planBuildContext.setRoot(plan.getRoot());
        planBuildContext.setSchemaTableNameList(schemaTableNameList);
        plan = applyRules(plan);
        return plan;
    }

    private PlanNode buildHelper()
    {
        if (schemaTableNameList.size() == 1) {
            return new TableNode(idAllocator.getNextId(), schemaTableNameList.get(0));
        }

        PlanNode firstTable = new TableNode(idAllocator.getNextId(), schemaTableNameList.get(0));
        PlanNode secondTable = new TableNode(idAllocator.getNextId(), schemaTableNameList.get(1));
        PlanNode joinNode = new JoinNode(idAllocator.getNextId(), firstTable, secondTable);

        for (int i = 2; i < schemaTableNameList.size(); i++) {
            PlanNode tableNode = new TableNode(idAllocator.getNextId(), schemaTableNameList.get(i));
            joinNode = new JoinNode(idAllocator.getNextId(), joinNode, tableNode);
        }
        joinNode.setRoot();
        return joinNode;
    }

    private PlanNode buildWithOrderedGraph()
    {
        List<MultiwayJoinNode> multiwayJoinNodeList = orderedGraph.getTraversalList();
        if (multiwayJoinNodeList.size() == 1) {
            return new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(0));
        }

        PlanNode firstTable = new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(0));
        PlanNode secondTable = new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(1));
        PlanNode joinNode = new JoinNode(idAllocator.getNextId(), firstTable, secondTable);

        for (int i = 2; i < multiwayJoinNodeList.size(); i++) {
            PlanNode tableNode = new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(i));
            joinNode = new JoinNode(idAllocator.getNextId(), joinNode, tableNode);
        }
        joinNode.setRoot();
        return joinNode;
    }

    private Plan applyRules(Plan plan)
    {
        for (Rule rule : rules) {
            plan = rule.applyToLogicalPlan(plan, planBuildContext);
        }
        return plan;
    }

    public PlanBuildContext getPlanBuildContext()
    {
        return planBuildContext;
    }
}
