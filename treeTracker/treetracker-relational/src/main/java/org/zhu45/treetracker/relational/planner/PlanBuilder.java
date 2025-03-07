package org.zhu45.treetracker.relational.planner;

import lombok.Getter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.plan.AggregateNode;
import org.zhu45.treetracker.relational.planner.plan.GatherMergeNode;
import org.zhu45.treetracker.relational.planner.plan.GatherNode;
import org.zhu45.treetracker.relational.planner.plan.HashNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.MaterializeNode;
import org.zhu45.treetracker.relational.planner.plan.Side;
import org.zhu45.treetracker.relational.planner.plan.SortNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.rule.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.logging.log4j.core.util.Assert.requireNonEmpty;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.zhu45.treetracker.common.StandardErrorCode.INVALID_USAGE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.ACTUAL_ROWS;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.AGGREGATE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.INNER;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.NODE_TYPE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.OUTER;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.PARENT_RELATIONSHIP;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.PLAN;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.PLANS;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.PLAN_ROWS;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.RELATION_NAME;

/**
 * PlanBuilder takes a list of table names or a query graph
 * and builds a logical plan out of it. The logical plan
 * is a left-deep query plan by default. The exception is when PlanBuildOption.Postgres is set, in which PlanBuilder
 * builds a plan based on the input PostgresPlan. Since we're only interested in
 * trying out different binary operator implementation and some optimization
 * technique on physical plan execution, we don't need fancy stuff like walk through AST
 * to perform semantic analysis and construct query plan (e.g., Presto).
 */
public class PlanBuilder
{
    private List<SchemaTableName> schemaTableNameList;
    private final PlanNodeIdAllocator idAllocator;
    @Getter
    private final PlanBuildContext planBuildContext;
    private final List<Rule> rules;

    public PlanBuilder(List<SchemaTableName> schemaTableNameList, PlanBuildContext context)
    {
        this.schemaTableNameList = requireNonNull(schemaTableNameList, "schemaTableNameList is null");
        requireNonEmpty(schemaTableNameList, "schemaTableNameList is empty");
        this.planBuildContext = requireNonNull(context, "context is null");
        this.idAllocator = requireNonNull(context.getPlanNodeIdAllocator(), "idAllocator is null");
        this.rules = requireNonNull(context.getRules(), "rules is null");
    }

    public PlanBuilder(PlanBuildContext context)
    {
        this.planBuildContext = requireNonNull(context, "context is null");
        this.idAllocator = requireNonNull(context.getPlanNodeIdAllocator(), "idAllocator is null");
        this.rules = requireNonNull(context.getRules(), "rules is null");
    }

    /**
     * Creates a logical plan that has left-deep join tree structure
     *
     * @return a logical plan
     */
    public Plan build()
    {
        Plan plan;
        switch (planBuildContext.getPlanBuildOption()) {
            case JOINTREE:
                plan = new Plan(buildWithOrderedGraph());
                break;
            case POSTGRES:
                plan = new Plan(buildFromPostgresPlan());
                break;
            default:
                plan = new Plan(buildHelper());
                break;
        }
        planBuildContext.setRoot(plan.getRoot());
        planBuildContext.setSchemaTableNameList(schemaTableNameList);
        plan = applyRules(plan);
        return plan;
    }

    private PlanNode buildHelper()
    {
        if (schemaTableNameList.size() == 1) {
            return new TableNode(idAllocator.getNextId(), schemaTableNameList.get(0), Side.OUTER);
        }

        PlanNode firstTable = new TableNode(idAllocator.getNextId(), schemaTableNameList.get(0), Side.OUTER);
        PlanNode secondTable = new TableNode(idAllocator.getNextId(), schemaTableNameList.get(1), Side.INNER);
        PlanNode joinNode = new JoinNode(idAllocator.getNextId(), firstTable, secondTable, Side.OUTER);

        for (int i = 2; i < schemaTableNameList.size(); i++) {
            PlanNode tableNode = new TableNode(idAllocator.getNextId(), schemaTableNameList.get(i), Side.INNER);
            joinNode = new JoinNode(idAllocator.getNextId(), joinNode, tableNode, Side.OUTER);
        }
        joinNode.setRoot();
        return joinNode;
    }

    private PlanNode buildWithOrderedGraph()
    {
        MultiwayJoinOrderedGraph orderedGraph = requireNonNull(planBuildContext.getOrderedGraph(),
                "orderedGraph is null");
        this.schemaTableNameList = orderedGraph.getTraversalList().stream().map(MultiwayJoinNode::getSchemaTableName)
                .collect(Collectors.toList());
        List<MultiwayJoinNode> multiwayJoinNodeList = orderedGraph.getTraversalList();
        if (multiwayJoinNodeList.size() == 1) {
            return new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(0), Side.OUTER);
        }

        PlanNode firstTable = new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(0), Side.OUTER);
        PlanNode secondTable = new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(1), Side.INNER);
        PlanNode joinNode = new JoinNode(idAllocator.getNextId(), firstTable, secondTable, Side.OUTER);

        for (int i = 2; i < multiwayJoinNodeList.size(); i++) {
            PlanNode tableNode = new TableNode(idAllocator.getNextId(), multiwayJoinNodeList.get(i), Side.INNER);
            joinNode = new JoinNode(idAllocator.getNextId(), joinNode, tableNode, Side.OUTER);
        }
        joinNode.setRoot();
        return joinNode;
    }

    private PlanNode buildFromPostgresPlan()
    {
        requireNonNull(planBuildContext.getPostgresPlan(), "PostgresPlan is unset");
        boolean needToBuildSchemaTableNameList = false;
        if (planBuildContext.getSchemaTableNameList() != null) {
            schemaTableNameList = planBuildContext.getSchemaTableNameList();
        }
        else {
            schemaTableNameList = new ArrayList<>();
            needToBuildSchemaTableNameList = true;
        }
        try {
            Object obj = new JSONParser().parse(planBuildContext.getPostgresPlan());
            JSONArray jo = (JSONArray) obj;
            PlanNode root = buildFromPostgresPlanHelper((JSONObject) ((JSONObject) jo.get(0)).get(PLAN.getValue()), needToBuildSchemaTableNameList);
            PlanNode rootOperatorPlanNode = findRootOperatorPlanNode(root);
            correctSide(rootOperatorPlanNode);
            rootOperatorPlanNode.setRoot();
            planBuildContext.setPostgresPlanRoot(root);
            // We want the root of the Plan to be the PlanNode that binds to the root of the operator tree.
            return rootOperatorPlanNode;
        }
        catch (ParseException e) {
            throw new TreeTrackerException(INVALID_USAGE, "postgresPlan cannot be parsed as JSON\n" + e);
        }
    }

    private Side obtainSide(String sideStr, PostgresPlanProperty nodeType)
    {
        checkArgument(sideStr != null || nodeType == AGGREGATE,
                "sideStr is null in a none-aggregate node: " + nodeType);
        // In JOB 1a, we encounter a situation where the root of the plan is Aggregate, and it doesn't have
        // Inner or Outer set. We assert that's the only case sideStr can be null. In such case, set the side
        // to Outer will have no harm.
        if (sideStr == null || sideStr.equals(OUTER.getValue())) {
            return Side.OUTER;
        }
        else if (sideStr.equals(INNER.getValue())) {
            return Side.INNER;
        }
        else {
            throw new TreeTrackerException(INVALID_USAGE, "sideStr is " + sideStr);
        }
    }

    private void setRows(JSONObject planRoot, PlanNode node)
    {
        // We prioritize to set actual rows if the number is available in the plan because
        // the number is the most accurate one compared to plan rows. As a result, if actual rows is
        // available, we discard the plan rows even if it appears in the plan.
        if (planRoot.containsKey(ACTUAL_ROWS.getValue())) {
            node.setActualRows((Long) planRoot.get(ACTUAL_ROWS.getValue()));
        }
        else if (planRoot.containsKey(PLAN_ROWS.getValue())) {
            node.setPlanRows((Long) planRoot.get(PLAN_ROWS.getValue()));
        }
    }

    private PlanNode buildFromPostgresPlanHelper(JSONObject planRoot, boolean needToBuildSchemaTableNameList)
    {
        JSONArray plans;
        PlanNode childPlanNode;
        PostgresPlanProperty nodeType = PostgresPlanProperty.getNodeType((String) planRoot.get(PostgresPlanProperty.NODE_TYPE.getValue()));
        switch (nodeType) {
            case HASH:
                plans = (JSONArray) planRoot.get(PLANS.getValue());
                checkState(plans.size() == 1, "Hash NodeType has more than one plans");
                checkState(planRoot.get(PARENT_RELATIONSHIP.getValue()).equals(INNER.getValue()), "Hash NodeType is not build side");
                childPlanNode = buildFromPostgresPlanHelper((JSONObject) plans.get(0), needToBuildSchemaTableNameList);
                HashNode hashNode = new HashNode(idAllocator.getNextId(), childPlanNode, Side.INNER);
                setRows(planRoot, hashNode);
                return hashNode;
            case SEQ_SCAN:
                checkState(planRoot.containsKey(RELATION_NAME.getValue()), "We run into an unexpected node: " + planRoot);
                TableNode tableNode = null;
                if (needToBuildSchemaTableNameList) {
                    SchemaTableName schemaTableName = new SchemaTableName(planBuildContext.getSchema(),
                            (String) planRoot.get(RELATION_NAME.getValue()));
                    schemaTableNameList.add(schemaTableName);
                    tableNode = new TableNode(idAllocator.getNextId(), schemaTableName,
                            obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType));
                }
                else {
                    SchemaTableName schemaTableName = schemaTableNameList.stream().filter(c -> c.getTableName()
                                    .equals(planRoot.get(RELATION_NAME.getValue())))
                            .findFirst()
                            .orElseThrow(() -> new TreeTrackerException(GENERIC_INTERNAL_ERROR, String.format("cannot find %s in given schemaTableNameList: %s", planRoot.get(RELATION_NAME.getValue()), schemaTableNameList)));
                    tableNode = new TableNode(idAllocator.getNextId(), schemaTableName,
                            obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType));
                }
                setRows(planRoot, tableNode);
                return tableNode;
            case HASH_JOIN:
            case NESTED_LOOP:
            case MERGE_JOIN:
                plans = (JSONArray) planRoot.get(PLANS.getValue());
                checkState(plans.size() == 2, "Join NodeType doesn't have exactly two plans");
                PlanNode node1 = buildFromPostgresPlanHelper((JSONObject) plans.get(0), needToBuildSchemaTableNameList);
                PlanNode node2 = buildFromPostgresPlanHelper((JSONObject) plans.get(1), needToBuildSchemaTableNameList);
                Side joinSide = obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType);
                JoinNode joinNode;
                // We always keep the left of join node to be Side.OUTER
                if (node1.getSide() == Side.INNER) {
                    checkState(node2.getSide() == Side.OUTER, "node2 is not probe side");
                    joinNode = new JoinNode(idAllocator.getNextId(), node2, node1, joinSide);
                }
                else {
                    checkState(node1.getSide() == Side.OUTER, "node1 is not probe side");
                    joinNode = new JoinNode(idAllocator.getNextId(), node1, node2, joinSide);
                }
                setRows(planRoot, joinNode);
                return joinNode;
            case AGGREGATE:
                plans = (JSONArray) planRoot.get(PLANS.getValue());
                checkState(plans.size() == 1, planRoot.get(NODE_TYPE.getValue()) + " NodeType has more than one plans");
                childPlanNode = buildFromPostgresPlanHelper((JSONObject) plans.get(0), needToBuildSchemaTableNameList);
                AggregateNode aggregateNode = new AggregateNode(idAllocator.getNextId(), childPlanNode,
                        obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType));
                setRows(planRoot, aggregateNode);
                return aggregateNode;
            case GATHER:
                plans = (JSONArray) planRoot.get(PLANS.getValue());
                checkState(plans.size() == 1, "Gather NodeType has more than one plans");
                childPlanNode = buildFromPostgresPlanHelper((JSONObject) plans.get(0), needToBuildSchemaTableNameList);
                GatherNode gatherNode = new GatherNode(idAllocator.getNextId(), childPlanNode,
                        obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType));
                setRows(planRoot, gatherNode);
                return gatherNode;
            case GATHER_MERGE:
                plans = (JSONArray) planRoot.get(PLANS.getValue());
                checkState(plans.size() == 1, "Gather NodeType has more than one plans");
                childPlanNode = buildFromPostgresPlanHelper((JSONObject) plans.get(0), needToBuildSchemaTableNameList);
                GatherMergeNode gatherMergeNode = new GatherMergeNode(idAllocator.getNextId(), childPlanNode,
                        obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType));
                setRows(planRoot, gatherMergeNode);
                return gatherMergeNode;
            case SORT:
                plans = (JSONArray) planRoot.get(PLANS.getValue());
                checkState(plans.size() == 1, "Sort NodeType has more than one plans");
                childPlanNode = buildFromPostgresPlanHelper((JSONObject) plans.get(0), needToBuildSchemaTableNameList);
                SortNode sortNode = new SortNode(idAllocator.getNextId(), childPlanNode,
                        obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType));
                setRows(planRoot, sortNode);
                return sortNode;
            case MATERIALIZE:
                plans = (JSONArray) planRoot.get(PLANS.getValue());
                checkState(plans.size() == 1, "Materialize NodeType has more than one plans");
                childPlanNode = buildFromPostgresPlanHelper((JSONObject) plans.get(0), needToBuildSchemaTableNameList);
                MaterializeNode materializeNode = new MaterializeNode(idAllocator.getNextId(), childPlanNode,
                        obtainSide((String) planRoot.get(PARENT_RELATIONSHIP.getValue()), nodeType));
                setRows(planRoot, materializeNode);
                return materializeNode;
            default:
                throw new TreeTrackerException(GENERIC_INTERNAL_ERROR,
                        "Unexpected NodeType: " + planRoot.get(NODE_TYPE.getValue()));
        }
    }

    /**
     * Postgres plan has a situation where a node is Side.INNER but its child could be Side.OUTER. That is, in Postgres plan,
     * there is a special case where HASH can have different side than its underlying JOIN node. For example, suppose we have
     * // JOIN,Outer
     * // |TAB,caseone_R,Outer
     * // |SORT,Inner
     * // ||GATHER,Outer
     * // |||JOIN,Outer
     * // ||||TAB,caseone_T,Outer
     * // ||||TAB,caseone_S,Inner
     * Then, SORT is encountered first compared to its children, grandchildren, etc. It has the correct side Inner.
     * However, the grandchild of SORT, JOIN, has side Outer, which could be troublesome for our later constructing physical plan
     * because the operator for JOIN should have Side.INNER, and we cannot directly set its Side by looking up its corresponding
     * PlanNode Side. This method aims to make sure JOIN in this example is also has Side.INNER.
     */
    private void correctSide(PlanNode root)
    {
        switch (root.getNodeType()) {
            case table:
                break;
            case join:
                for (PlanNode child : root.getSources()) {
                    correctSide(child);
                }
                break;
            case hash:
            case gather:
            case sort:
            case materialize:
            case gather_merge:
            case aggregate:
                checkState(root.getSources().size() == 1, root.getNodeType() + " node must have one child");
                PlanNode targetNode = root.getSources().get(0);
                while (targetNode.getNodeType() != OptType.join &&
                        targetNode.getNodeType() != OptType.table) {
                    checkState(targetNode.getSources().size() == 1, targetNode
                            + " has unexpected more than 1 child");
                    targetNode = targetNode.getSources().get(0);
                }
                targetNode.setSide(root.getSide());
                correctSide(targetNode);
                break;
            case fullReducer:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * This function aims to find the PlanNode that will have the root operator of the operator tree.
     */
    public static PlanNode findRootOperatorPlanNode(PlanNode planRoot)
    {
        switch (planRoot.getNodeType()) {
            case gather:
            case hash:
            case sort:
            case aggregate:
            case materialize:
            case gather_merge:
                checkState(planRoot.getSources().size() == 1, planRoot.getNodeType() + "has more than one child");
                return findRootOperatorPlanNode(planRoot.getSources().get(0));
            case join:
            case table:
                return planRoot;
            default:
                throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, "cannot find RootOperatorPlanNode for the given " + planRoot);
        }
    }

    private Plan applyRules(Plan plan)
    {
        for (Rule rule : rules) {
            plan = rule.applyToLogicalPlan(plan, planBuildContext);
        }
        return plan;
    }
}
