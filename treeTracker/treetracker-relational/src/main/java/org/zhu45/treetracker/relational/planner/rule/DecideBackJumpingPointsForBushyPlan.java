package org.zhu45.treetracker.relational.planner.rule;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.CheckLabelConnectedness;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.JoinResultColumnHandle;
import org.zhu45.treetracker.relational.OperatorSpecification;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanVisitor;
import org.zhu45.treetracker.relational.planner.catalog.TableCatalog;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeHeightProvider;
import org.zhu45.treetracker.relational.planner.plan.AggregateNode;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.GatherMergeNode;
import org.zhu45.treetracker.relational.planner.plan.GatherNode;
import org.zhu45.treetracker.relational.planner.plan.HashNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.MaterializeNode;
import org.zhu45.treetracker.relational.planner.plan.Side;
import org.zhu45.treetracker.relational.planner.plan.SortNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.reverse;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.Utils.formatTraceMessageWithDepth;
import static org.zhu45.treetracker.relational.planner.JoinTreeGenerator.fillNodeConntectedness;
import static org.zhu45.treetracker.relational.planner.JoinTreeGenerator.fillNodeType;
import static org.zhu45.treetracker.relational.planner.PlanBuilder.findRootOperatorPlanNode;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.getLeftMostNode;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.ifUseTTJFamilyOperator;
import static org.zhu45.treetracker.relational.planner.rule.DecideBackJumpingPointsForBushyPlan.CollectLeftDeepPlansContext.printLeftDeepPlans;

/**
 * This rule effectively tries to construct a join tree for a given bushy plan. The result
 * is an explicit join tree constructed from the bushy plan. It is up to specific operator on how to consume
 * such join tree. For example, TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator may translate join
 * tree as backjumping points (CIDR 25' submission terminology).
 */
public class DecideBackJumpingPointsForBushyPlan
        extends BaseRule
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(DecideBackJumpingPointsForBushyPlan.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private RuleStatistics.Builder builder = RuleStatistics.builder(this.getClass());

    public static final String tempPrefix = "temp_";

    @Override
    public Plan applyToLogicalPlan(Plan plan, PlanBuildContext context)
    {
        return plan;
    }

    /**
     * The idea is to extract all the left-deep plans from the given bushy plan. Then, for each
     * left-deep plan, we invoke the shallow tree heuristic to construct the corresponding join tree.
     * We finish the whole process once we finish processing all the left-deep plans. We concatenate all join subtrees with
     * each one corresponding to left-deep subplan together to form the final join tree. Sometimes, the left-deep subplan may
     * not correspond to a join tree unless we consider any relation of the subplan that represents intermediate results as a
     * virtual relation. As a result, we have join forest instead of trees after we finish processing the bushy plan. During
     * the binding phase where we bind join tree(s) with the operators, each operator may access different join trees.
     */
    @Override
    public Plan applyToPhysicalPlan(PlanNode node, RuleContext context)
    {
        ruleContext = context;
        List<SchemaTableName> originSchemaTableNameList = ruleContext.getPlanBuildContext().getSchemaTableNameList();
        CollectLeftDeepPlansResult result = collectLeftDeepPlansFromBushyPlan(node);
        Pair<Stack<PlanNode>, Stack<MultiwayJoinOrderedGraph>> pair = decideBackJumpingPointsForLeftDeepPlan(result);
        if (pair.getKey().isEmpty()) {
            MultiwayJoinOrderedGraph joinTree = assembleJoinTrees(pair.getValue());
            checkState(pair.getValue().isEmpty(), "Un-assembled join trees exist");
            ruleContext.getPlanBuildContext().setOrderedGraph(joinTree);
            bindMultiwayJoinNodeToOperators(node, joinTree);
            ruleStatistics = builder
                    .optimalJoinTree(joinTree)
                    .build();
        }
        else {
            Stack<MultiwayJoinOrderedGraph> resultJoinTrees = assembleJoinTreesIntoOnes(node, pair);
            checkState(pair.getValue().isEmpty(), "Un-assembled join trees exist");
            builder.node2JoinTrees(constructNode2JoinTrees(node, pair.getKey(), resultJoinTrees));
            bindMultiwayJoinNodeFromTreesToOperators(node, pair.getKey(), resultJoinTrees);
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                PlanPrinter printer = new PlanPrinter(node);
                traceLogger.debug(printer.toText(0));
            }
            ruleStatistics = builder.build();
        }
        ruleContext.getPlanBuildContext().setPlanBuildOption(PlanBuildContext.PlanBuildOption.POSTGRES);
        ruleContext.getPlanBuildContext().setSchemaTableNameList(originSchemaTableNameList);
        return ruleContext.getPlan();
    }

    public static class CollectLeftDeepPlansContext
    {
        Stack<Plan> leftDeepPlans = new Stack<>();
        int traceDepth;

        public static String printLeftDeepPlans(List<Plan> leftDeepPlans)
        {
            StringBuilder builder = new StringBuilder();
            for (Plan plan : leftDeepPlans) {
                PlanPrinter printer = new PlanPrinter(plan.getRoot());
                builder.append(printer.toText(0))
                        .append("\n---\n");
            }
            return builder.toString();
        }
    }

    public static class CollectLeftDeepPlansResult
    {
        List<Plan> leftDeepPlans;
        HashMap<PlanNode, List<SchemaTableName>> nodeToSchemaTableNames;

        public CollectLeftDeepPlansResult(List<Plan> leftDeepPlans,
                                          HashMap<PlanNode, List<SchemaTableName>> nodeToSchemaTableNames)
        {
            this.leftDeepPlans = leftDeepPlans;
            this.nodeToSchemaTableNames = nodeToSchemaTableNames;
        }
    }

    public static CollectLeftDeepPlansResult collectLeftDeepPlansFromBushyPlan(PlanNode root)
    {
        HashMap<PlanNode, List<SchemaTableName>> nodeToSchemaTableNames = new HashMap<>();
        PlanVisitor<PlanNode, CollectLeftDeepPlansContext> visitor = new PlanVisitor<>()
        {
            @Override
            public PlanNode visitPlan(PlanNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter visitPlan", context.traceDepth));
                }
                PlanNode ret = node.accept(this, context);
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("return: " + ret, context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("leave visitPlan", context.traceDepth));
                    context.traceDepth--;
                }
                return ret;
            }

            @Override
            public PlanNode visitJoin(JoinNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter visitJoin", context.traceDepth));
                }
                nodeToSchemaTableNames.computeIfAbsent(node, k -> new ArrayList<>());
                PlanNode rightNode = visitPlan(requireNonNull(node.getRight()), context);
                PlanNode leftNode = visitPlan(requireNonNull(node.getLeft()), context);
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("leftNode: " + leftNode, context.traceDepth));
                }
                if (leftNode.getNodeType() == OptType.join) {
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessageWithDepth("context.leftDeepPlans before popping: " +
                                printLeftDeepPlans(context.leftDeepPlans), context.traceDepth));
                    }
                    context.leftDeepPlans.pop();
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessageWithDepth("context.leftDeepPlans after popping: " +
                                printLeftDeepPlans(context.leftDeepPlans), context.traceDepth));
                    }
                    nodeToSchemaTableNames.get(node).addAll(nodeToSchemaTableNames.get(leftNode));
                    nodeToSchemaTableNames.remove(leftNode);
                }
                else {
                    checkState(leftNode.getNodeType() == OptType.table, "unexpected node type: " + leftNode.getNodeType());
                    nodeToSchemaTableNames.get(node).add(leftNode.getSchemaTableName());
                }
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("context.leftDeepPlans before pushing: " +
                            printLeftDeepPlans(context.leftDeepPlans), context.traceDepth));
                }
                context.leftDeepPlans.push(new Plan(node));
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("context.leftDeepPlans after pushing: " +
                            printLeftDeepPlans(context.leftDeepPlans), context.traceDepth));
                }
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("leave visitJoin", context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("return : " + node, context.traceDepth));
                    context.traceDepth--;
                }
                if (rightNode.getNodeType() != OptType.table) {
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessageWithDepth("rightNode: " + rightNode, context.traceDepth));
                    }
                    PlanNode rootRightSubPlan = findRootOperatorPlanNode(rightNode);
                    PlanNode current = getLeftMostNode(rootRightSubPlan);
                    nodeToSchemaTableNames.get(node).add(current.getSchemaTableName());
                    // We set the SchemaTableName to be the left-most node of the subtree rooted at rootRightSubPlan.
                    // The reason to do so is that we want to make sure we can invoke FindOptimalJoinTree rule.
                    checkState(current.getNodeType() == OptType.table,
                            current + " has to be a table node");
                    rootRightSubPlan.setSchemaTableName(current.getSchemaTableName());
                }
                else {
                    nodeToSchemaTableNames.get(node).add(rightNode.getSchemaTableName());
                }
                return node;
            }

            @Override
            public PlanNode visitTable(TableNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter TableNode: " + node, context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("leave TableNode: " + node, context.traceDepth));
                    context.traceDepth--;
                }
                return node;
            }

            @Override
            public PlanNode visitHash(HashNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter HashNode: " + node, context.traceDepth));
                }
                checkState(node.getSources().size() == 1, node + " has more than one child");
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("leave HashNode: " + node, context.traceDepth));
                    context.traceDepth--;
                }
                return visitPlan(node.getSources().get(0), context);
            }

            @Override
            public PlanNode visitGather(GatherNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter GatherNode: " + node, context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("leave GatherNode: " + node, context.traceDepth));
                    context.traceDepth--;
                }
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return visitPlan(node.getSources().get(0), context);
            }

            @Override
            public PlanNode visitAggregate(AggregateNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter AggregateNode: " + node, context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("leave AggregateNode: " + node, context.traceDepth));
                    context.traceDepth--;
                }
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return visitPlan(node.getSources().get(0), context);
            }

            @Override
            public PlanNode visitGatherMerge(GatherMergeNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter GatherMergeNode: " + node, context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("leave GatherMergeNode: " + node, context.traceDepth));
                    context.traceDepth--;
                }
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return node.getSources().get(0);
            }

            @Override
            public PlanNode visitMaterialize(MaterializeNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter MaterializeNode: " + node, context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("leave MaterializeNode: " + node, context.traceDepth));
                    context.traceDepth--;
                }
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return node.getSources().get(0);
            }

            @Override
            public PlanNode visitSort(SortNode node, CollectLeftDeepPlansContext context)
            {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    context.traceDepth++;
                    traceLogger.trace(formatTraceMessageWithDepth("enter SortNode: " + node, context.traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("leave SortNode: " + node, context.traceDepth));
                    context.traceDepth--;
                }
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return node.getSources().get(0);
            }

            @Override
            public PlanNode visitFullReducer(FullReducerNode node, CollectLeftDeepPlansContext context)
            {
                throw new UnsupportedOperationException();
            }
        };
        CollectLeftDeepPlansContext context = new CollectLeftDeepPlansContext();
        visitor.visitPlan(root, context);
        return new CollectLeftDeepPlansResult(context.leftDeepPlans, nodeToSchemaTableNames);
    }

    private Pair<Stack<PlanNode>, Stack<MultiwayJoinOrderedGraph>> decideBackJumpingPointsForLeftDeepPlan(CollectLeftDeepPlansResult result)
    {
        Stack<MultiwayJoinOrderedGraph> joinTrees = new Stack<>();
        Stack<PlanNode> virtualRelationRoots = new Stack<>();
        for (Plan leftDeepPlan : result.leftDeepPlans) {
            try {
                MultiwayJoinOrderedGraph optimalJoinTree = findOptimalJoinTree(leftDeepPlan, result, null);
                joinTrees.add(optimalJoinTree);
            }
            catch (IllegalStateException e) {
                if (e.getMessage().contains("scanList is empty")) {
                    // Since we cannot construct join tree for this left-deep plan by using the input relations,
                    // we aggressively replace all the right child of join that is also join (right join child) with intermediate relations.
                    // Then, we try to construct join tree again. Note that we only temporarily update attributes of the associating
                    // relation of the right join child for the sake of constructing join tree.
                    // TODO: we don't have to replace all the right child of join that is also join with intermediate relations because sometimes, right child of join that is also
                    //  join can correspond to a join subtree by using the left-most relation as the root of the subplan (e.g., TPC-H 8W the right subplan with nation and region)
                    Triple<List<SchemaTableName>, List<PlanNode>, HashMap<SchemaTableName, HashSet<String>>> triple = obtainSchemaTableNameAttributes(leftDeepPlan.getRoot(), ruleContext.getPlanBuildContext());
                    MultiwayJoinOrderedGraph optimalJoinTree = findOptimalJoinTree(leftDeepPlan, result, Pair.of(triple.getLeft(), triple.getRight()));
                    joinTrees.add(optimalJoinTree);
                    virtualRelationRoots.addAll(triple.getMiddle());
                }
                else {
                    throw e;
                }
            }
        }
        return Pair.of(virtualRelationRoots, joinTrees);
    }

    private MultiwayJoinOrderedGraph findOptimalJoinTree(Plan leftDeepPlan,
                                                         CollectLeftDeepPlansResult result,
                                                         Pair<List<SchemaTableName>, HashMap<SchemaTableName, HashSet<String>>> pair)
    {
        FindOptimalJoinTree shallowTreeHeuristic = new FindOptimalJoinTree(new JoinTreeHeightProvider());
        ruleContext.setNotGeneratePlan(true);
        if (pair != null) {
            ruleContext.getPlanBuildContext().setSchemaTableNameAttributes(pair.getRight());
            ruleContext.getPlanBuildContext().setSchemaTableNameList(pair.getLeft());
        }
        else {
            ruleContext.getPlanBuildContext().setSchemaTableNameList(result.nodeToSchemaTableNames.get(leftDeepPlan.getRoot()));
            ruleContext.getPlanBuildContext().setSchemaTableNameAttributes(null);
        }
        checkState(shallowTreeHeuristic.checkForRulePrecondition(leftDeepPlan.getRoot()),
                "ShallowTreeHeuristic is not applicable towards the plan with root: " + leftDeepPlan.getRoot());
        shallowTreeHeuristic.applyToPhysicalPlan(leftDeepPlan.getRoot(), ruleContext);
        return shallowTreeHeuristic.ruleStatistics.getOptimalJoinTree();
    }

    private Triple<List<SchemaTableName>, List<PlanNode>, HashMap<SchemaTableName, HashSet<String>>> obtainSchemaTableNameAttributes(PlanNode rootNode,
                                                                                                                                     PlanBuildContext context)
    {
        HashMap<SchemaTableName, HashSet<String>> schemaTableNameAttributes = new HashMap<>();
        List<SchemaTableName> schemaTableNames = new ArrayList<>();
        List<PlanNode> virtualRelationNodes = new ArrayList<>();
        PlanNode currNode = rootNode;
        while (!currNode.getSources().isEmpty()) {
            if (currNode.getNodeType() == OptType.join) {
                PlanNode rightChild = ((JoinNode) currNode).getRight();
                PlanNode rootRightSubPlan = findRootOperatorPlanNode(rightChild);
                if (rootRightSubPlan.getNodeType() == OptType.join) {
                    List<String> tempAttr = rootRightSubPlan.getOperator()
                            .getResultColumnHandles()
                            .stream()
                            .map(JoinResultColumnHandle::getColumnName)
                            .collect(Collectors.toList());
                    SchemaTableName tempRelation = registerTempRelation(rootRightSubPlan.getSchemaTableName(), rootRightSubPlan.getOperator().getResultColumnHandles(), context);
                    schemaTableNameAttributes.put(tempRelation, new HashSet<>(tempAttr));
                    schemaTableNames.add(tempRelation);
                    virtualRelationNodes.add(rootRightSubPlan);
                }
                else {
                    checkState(rootRightSubPlan.getNodeType() == OptType.table);
                    schemaTableNameAttributes.put(rootRightSubPlan.getSchemaTableName(),
                            new HashSet<>(context.getCatalogGroup().getTableCatalog(rootRightSubPlan.getSchemaTableName()).getAttributes()));
                    schemaTableNames.add(rootRightSubPlan.getSchemaTableName());
                }
            }
            currNode = currNode.getSources().get(0);
        }
        if (currNode.getNodeType() == OptType.table) {
            schemaTableNames.add(currNode.getSchemaTableName());
            schemaTableNameAttributes.put(currNode.getSchemaTableName(),
                    new HashSet<>(context.getCatalogGroup().getTableCatalog(currNode.getSchemaTableName()).getAttributes()));
        }
        reverse(schemaTableNames);
        return Triple.of(schemaTableNames, virtualRelationNodes, schemaTableNameAttributes);
    }

    private MultiwayJoinOrderedGraph assembleJoinTrees(Stack<MultiwayJoinOrderedGraph> joinTrees)
    {
        MultiwayJoinOrderedGraph joinTree = joinTrees.pop();
        assembleJoinTreesHelper(joinTree, joinTrees);
        fillNodeConntectedness(joinTree);
        fillNodeType(joinTree);
        joinTree.setDepth();
        checkState(new CheckLabelConnectedness(joinTree.getTraversalList(), ruleContext.getJdbcClient()).check(),
                "\n" + joinTree + "\nis not a join tree");
        return joinTree;
    }

    private void assembleJoinTreesHelper(MultiwayJoinOrderedGraph joinTree, Stack<MultiwayJoinOrderedGraph> joinTrees)
    {
        for (MultiwayJoinNode node : joinTree.getTraversalList()) {
            if (joinTrees.isEmpty()) {
                return;
            }
            else if (joinTrees.peek().getRoot().getSchemaTableName().equals(node.getSchemaTableName())) {
                MultiwayJoinOrderedGraph nextJoinTree = joinTrees.pop();
                concatenateJoinTrees(joinTree, nextJoinTree);
                assembleJoinTreesHelper(joinTree, joinTrees);
            }
        }
    }

    private Stack<MultiwayJoinOrderedGraph> assembleJoinTreesIntoOnes(PlanNode root, Pair<Stack<PlanNode>, Stack<MultiwayJoinOrderedGraph>> pair)
    {
        Stack<MultiwayJoinOrderedGraph> joinTrees = pair.getValue();
        MultiwayJoinOrderedGraph joinTree = joinTrees.pop();
        Stack<MultiwayJoinOrderedGraph> resultJoinTrees = new Stack<>();
        assembleJoinTreesIntoOnesHelper(joinTree, joinTrees, resultJoinTrees);
        for (MultiwayJoinOrderedGraph resultJoinTree : resultJoinTrees) {
            fillNodeConntectedness(resultJoinTree);
            fillNodeType(resultJoinTree);
            resultJoinTree.setDepth();
            checkState(new CheckLabelConnectedness(resultJoinTree.getTraversalList(), ruleContext.getJdbcClient()).check(),
                    "\n" + resultJoinTree + "\nis not a join tree");
        }
        Stack<MultiwayJoinOrderedGraph> returnJoinTrees = new Stack<>();
        // We need to make sure the order of join trees in resultJoinTrees stack is in sync with the order of PlanNodes in virtualRelationRoots
        // The order is to visit each left-deep subplan in the bottom-up fashion of their appearance in the original bushy plan, i.e., the top of the stack
        // is the outermost left-deep subplan (the root is the left-most relation of the bushy plan).
        List<PlanNode> allNodes = new ArrayList<>(pair.getKey());
        allNodes.add(root);
        for (PlanNode node : allNodes) {
            PlanNode leftMostNode = getLeftMostNode(node);
            SchemaTableName leftMostNodeSchemaTableName = leftMostNode.getSchemaTableName();
            for (MultiwayJoinOrderedGraph resultJoinTree : resultJoinTrees) {
                if (resultJoinTree.getRoot().getSchemaTableName().equals(leftMostNodeSchemaTableName)) {
                    returnJoinTrees.push(resultJoinTree);
                    break;
                }
            }
        }
        return returnJoinTrees;
    }

    private void assembleJoinTreesIntoOnesHelper(MultiwayJoinOrderedGraph joinTree,
                                                 Stack<MultiwayJoinOrderedGraph> joinTrees,
                                                 Stack<MultiwayJoinOrderedGraph> resultJoinTrees)
    {
        for (MultiwayJoinNode node : joinTree.getTraversalList()) {
            if (joinTrees.isEmpty()) {
                if (!resultJoinTrees.contains(joinTree)) {
                    resultJoinTrees.add(joinTree);
                }
                return;
            }
            else if (joinTrees.peek().getRoot().getSchemaTableName().equals(node.getSchemaTableName())) {
                MultiwayJoinOrderedGraph nextJoinTree = joinTrees.pop();
                concatenateJoinTrees(joinTree, nextJoinTree);
                assembleJoinTreesIntoOnesHelper(joinTree, joinTrees, resultJoinTrees);
            }
            else if (!isBaseRelation(node.getSchemaTableName()) &&
                    joinTrees.peek().getRoot().getSchemaTableName().equals(obtainBaseRelation(node.getSchemaTableName()))) {
                MultiwayJoinOrderedGraph nextJoinTree = joinTrees.pop();
                assembleJoinTreesIntoOnesHelper(nextJoinTree, joinTrees, resultJoinTrees);
            }
        }
        // This is needed because if the node in the for loop is the last node, and it triggers
        // a recursion call, which causes joinTrees becomes empty. Then, joinTree won't be added to the
        // resultJoinTrees, e.g., test4() in TestDecideBackJumpingPointsForBushyPlan.
        if (!resultJoinTrees.contains(joinTree)) {
            resultJoinTrees.add(joinTree);
        }
    }

    private void concatenateJoinTrees(MultiwayJoinOrderedGraph candidateJoinTree, MultiwayJoinOrderedGraph toBeMergedJoinTree)
    {
        int indexOfNode = 0;
        MultiwayJoinNode nodeInCandidate = null;
        for (int i = 0; i < candidateJoinTree.getTraversalList().size(); i++) {
            MultiwayJoinNode currNode = candidateJoinTree.getTraversalList().get(i);
            if (currNode.getSchemaTableName().equals(toBeMergedJoinTree.getRoot().getSchemaTableName())) {
                indexOfNode = i;
                nodeInCandidate = currNode;
                break;
            }
        }
        List<MultiwayJoinNode> newTraversalList = new ArrayList<>(candidateJoinTree.getTraversalList().subList(0, indexOfNode + 1));
        newTraversalList.addAll(toBeMergedJoinTree.getTraversalList().subList(1, toBeMergedJoinTree.getTraversalList().size()));
        newTraversalList.addAll(candidateJoinTree.getTraversalList().subList(indexOfNode + 1, candidateJoinTree.getTraversalList().size()));

        // Update parent
        for (MultiwayJoinNode node : toBeMergedJoinTree.getParent().keySet()) {
            if (!node.equals(toBeMergedJoinTree.getRoot())) {
                if (toBeMergedJoinTree.getParent().get(node).get(0).equals(toBeMergedJoinTree.getRoot())) {
                    candidateJoinTree.getParent().computeIfAbsent(node, k -> new ArrayList<>());
                    candidateJoinTree.getParent().get(node).add(nodeInCandidate);
                }
                else {
                    candidateJoinTree.getParent().computeIfAbsent(node, k -> new ArrayList<>());
                    candidateJoinTree.getParent().get(node).add(toBeMergedJoinTree.getParent().get(node).get(0));
                }
            }
        }

        // Update children
        for (MultiwayJoinNode node : toBeMergedJoinTree.getChildren().keySet()) {
            if (node.equals(toBeMergedJoinTree.getRoot())) {
                candidateJoinTree.getChildren().get(nodeInCandidate).addAll(toBeMergedJoinTree.getChildren().get(node));
            }
            else {
                candidateJoinTree.getChildren().computeIfAbsent(node, k -> new ArrayList<>());
                candidateJoinTree.getChildren().get(node).addAll(toBeMergedJoinTree.getChildren().get(node));
            }
        }

        candidateJoinTree.setTraversalList(newTraversalList);
        fillNodeConntectedness(candidateJoinTree);
        fillNodeType(candidateJoinTree);
        candidateJoinTree.setDepth();
        checkState(candidateJoinTree.validate());
    }

    /**
     * We need to setMultiwayJoinNode to both join and table scan operators.
     */
    private void bindMultiwayJoinNodeToOperators(PlanNode root, MultiwayJoinOrderedGraph joinTree)
    {
        bindMultiwayJoinNodeToOperatorsHelpers(root, joinTree, 0);
    }

    private Integer bindMultiwayJoinNodeToOperatorsHelpers(PlanNode root, MultiwayJoinOrderedGraph joinTree, Integer currentIdxOnTraversalList)
    {
        switch (root.getNodeType()) {
            case join:
                JoinNode joinNode = (JoinNode) root;
                currentIdxOnTraversalList = bindMultiwayJoinNodeToOperatorsHelpers(joinNode.getLeft(), joinTree, currentIdxOnTraversalList);
                if (joinNode.getOperator().getSide() == Side.INNER) {
                    joinNode.getOperator().setMultiwayJoinNode(getLeftMostNode(joinNode).getOperator().getMultiwayJoinNode());
                }
                currentIdxOnTraversalList = bindMultiwayJoinNodeToOperatorsHelpers(joinNode.getRight(), joinTree, currentIdxOnTraversalList);
                return currentIdxOnTraversalList;
            case table:
                root.getOperator().setMultiwayJoinNode(joinTree.getTraversalList().get(currentIdxOnTraversalList));
                currentIdxOnTraversalList += 1;
                return currentIdxOnTraversalList;
            case hash:
            case sort:
            case gather:
            case aggregate:
            case gather_merge:
            case materialize:
                return bindMultiwayJoinNodeToOperatorsHelpers(root.getSources().get(0), joinTree, currentIdxOnTraversalList);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void bindMultiwayJoinNodeFromTreesToOperators(PlanNode root,
                                                          Stack<PlanNode> virtualRelationRoots,
                                                          Stack<MultiwayJoinOrderedGraph> resultJoinTrees)
    {
        PlanBuildContext context = root.getOperator().getPlanBuildContext();
        MultiwayJoinOrderedGraph currentTree = resultJoinTrees.pop();
        context = new PlanBuildContext(context, currentTree);
        bindMultiwayJoinNodeFromTreesToOperatorsHelpers(root, currentTree, 0, virtualRelationRoots, context);
        while (!virtualRelationRoots.isEmpty()) {
            currentTree = resultJoinTrees.pop();
            PlanNode virtualRoot = virtualRelationRoots.pop();
            context = new PlanBuildContext(context, currentTree);
            bindMultiwayJoinNodeFromTreesToOperatorsHelpers(virtualRoot, currentTree, 0, virtualRelationRoots, context);
        }
    }

    private Integer bindMultiwayJoinNodeFromTreesToOperatorsHelpers(PlanNode root,
                                                                    MultiwayJoinOrderedGraph currentTree,
                                                                    Integer currentIdxOnTraversalList,
                                                                    Stack<PlanNode> virtualRelationRoots,
                                                                    PlanBuildContext context)
    {
        switch (root.getNodeType()) {
            case join:
                JoinNode joinNode = (JoinNode) root;
                root.getOperator().setPlanBuildContext(context);
                if (virtualRelationRoots.contains(root)) {
                    joinNode.setVirtualSchemaTableName(currentTree.getTraversalList().get(currentIdxOnTraversalList).getSchemaTableName());
                    joinNode.getOperator().setChildMultiwayJoinNode(currentTree.getTraversalList().get(currentIdxOnTraversalList));
                    currentIdxOnTraversalList += 1;
                    return currentIdxOnTraversalList;
                }
                else if (joinNode.getOperator().getSide() == Side.INNER) {
                    joinNode.setSchemaTableName(currentTree.getTraversalList().get(currentIdxOnTraversalList).getSchemaTableName());
                    joinNode.getOperator().setMultiwayJoinNode(currentTree.getTraversalList().get(currentIdxOnTraversalList));
                }
                currentIdxOnTraversalList = bindMultiwayJoinNodeFromTreesToOperatorsHelpers(joinNode.getLeft(), currentTree, currentIdxOnTraversalList, virtualRelationRoots, context);
                currentIdxOnTraversalList = bindMultiwayJoinNodeFromTreesToOperatorsHelpers(joinNode.getRight(), currentTree, currentIdxOnTraversalList, virtualRelationRoots, context);
                return currentIdxOnTraversalList;
            case table:
                checkState(root.getSchemaTableName() != null, root + " has no SchemaTableName set");
                root.getOperator().setMultiwayJoinNode(currentTree.getTraversalList().get(currentIdxOnTraversalList));
                root.getOperator().setPlanBuildContext(context);
                currentIdxOnTraversalList += 1;
                return currentIdxOnTraversalList;
            case hash:
            case sort:
            case gather:
            case aggregate:
            case gather_merge:
            case materialize:
                return bindMultiwayJoinNodeFromTreesToOperatorsHelpers(root.getSources().get(0), currentTree, currentIdxOnTraversalList, virtualRelationRoots, context);
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * This method calls initializeContextObject() method on the join operators so that TTJ family operators
     * can work correctly.
     * When saving planStatistics, we can save the root of the left-deep plans and corresponding join tree and then during loading,
     * we find the root of left-deep plans and properly set detection-guilty relationship through calling this function.
     */
    public static void initializeContextObject(PlanNode node)
    {
        PlanVisitor<PlanNode, Void> visitor = new PlanVisitor<>()
        {
            @Override
            public PlanNode visitPlan(PlanNode node, Void context)
            {
                return node.accept(this, context);
            }

            @Override
            public PlanNode visitJoin(JoinNode node, Void context)
            {
                node.getOperator().initializeContextObject();
                visitPlan(node.getLeft(), context);
                visitPlan(node.getRight(), context);
                return node;
            }

            @Override
            public PlanNode visitTable(TableNode node, Void context)
            {
                return node;
            }

            @Override
            public PlanNode visitHash(HashNode node, Void context)
            {
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return visitPlan(node.getSources().get(0), context);
            }

            @Override
            public PlanNode visitGather(GatherNode node, Void context)
            {
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return visitPlan(node.getSources().get(0), context);
            }

            @Override
            public PlanNode visitAggregate(AggregateNode node, Void context)
            {
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return visitPlan(node.getSources().get(0), context);
            }

            @Override
            public PlanNode visitGatherMerge(GatherMergeNode node, Void context)
            {
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return node.getSources().get(0);
            }

            @Override
            public PlanNode visitMaterialize(MaterializeNode node, Void context)
            {
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return node.getSources().get(0);
            }

            @Override
            public PlanNode visitSort(SortNode node, Void context)
            {
                checkState(node.getSources().size() == 1, node + " has more than one child");
                return node.getSources().get(0);
            }

            @Override
            public PlanNode visitFullReducer(FullReducerNode node, Void context)
            {
                throw new UnsupportedOperationException();
            }
        };
        visitor.visitPlan(node, null);
    }

    public static String obtainTempRelationName(String tableName)
    {
        return tempPrefix + tableName;
    }

    private static SchemaTableName obtainTempRelation(SchemaTableName schemaTableName)
    {
        return new SchemaTableName(schemaTableName.getSchemaName(), obtainTempRelationName(schemaTableName.getTableName()));
    }

    private static SchemaTableName obtainBaseRelation(SchemaTableName schemaTableName)
    {
        if (!isBaseRelation(schemaTableName)) {
            return new SchemaTableName(schemaTableName.getSchemaName(), schemaTableName.getTableName().substring(tempPrefix.length()));
        }
        return schemaTableName;
    }

    private static boolean isBaseRelation(SchemaTableName schemaTableName)
    {
        return !schemaTableName.getTableName().contains(tempPrefix);
    }

    private SchemaTableName registerTempRelation(SchemaTableName schemaTableName,
                                                 List<JoinResultColumnHandle> columnHandles,
                                                 PlanBuildContext context)
    {
        SchemaTableName temp = obtainTempRelation(schemaTableName);
        context.getCatalogGroup().addTableCatalog(temp, new TableCatalog(schemaTableName,
                columnHandles.stream().map(JoinResultColumnHandle::getColumnType).collect(Collectors.toList()),
                columnHandles.stream().map(JoinResultColumnHandle::getColumnName).collect(Collectors.toList())));
        return temp;
    }

    private LinkedHashMap<OperatorSpecification, MultiwayJoinOrderedGraph> constructNode2JoinTrees(PlanNode root,
                                                                                                   Stack<PlanNode> virtualRelationRoots,
                                                                                                   Stack<MultiwayJoinOrderedGraph> resultJoinTrees)
    {
        LinkedHashMap<OperatorSpecification, MultiwayJoinOrderedGraph> node2JoinTrees = new LinkedHashMap<>();
        List<MultiwayJoinOrderedGraph> resultJoinTreesList = new ArrayList<>(resultJoinTrees);
        List<PlanNode> virtualRelationRootsList = new ArrayList<>(virtualRelationRoots);
        node2JoinTrees.put(OperatorSpecification.intoSpecification(root), resultJoinTreesList.get(0));
        for (int i = 0; i < virtualRelationRootsList.size(); i++) {
            node2JoinTrees.put(OperatorSpecification.intoSpecification(virtualRelationRootsList.get(i)),
                    resultJoinTreesList.get(i + 1));
        }
        return node2JoinTrees;
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        checkState(node.getOperator().getPlanBuildContext().getPlanBuildOption() == PlanBuildContext.PlanBuildOption.POSTGRES,
                "the rule is only applicable towards " + PlanBuildContext.PlanBuildOption.POSTGRES);
        checkState(node.getNodeType() == OptType.join, node + " has to have " + OptType.join);
        checkState(ifUseTTJFamilyOperator(node.getOperator().getPlanBuildContext().getOperatorMap(), true),
                "The rule is only applicable towards TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator or " +
                        "TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator");
        return true;
    }

    @Override
    public RuleType getRuleType()
    {
        return RuleType.AS_A_WHOLE;
    }
}
