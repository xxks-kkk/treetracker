package org.zhu45.treetracker.relational.planner.rule;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.FullReducerOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanBuilder;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanVisitor;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.rule.AttachFullReducer.TableScanVisitor.gatherTableScanNodes;

/**
 * We modify the given plan by adding the full reducer node
 */
public class AttachFullReducer
        extends BaseRule
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(AttachFullReducer.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    Class<? extends TupleBasedJoinOperator> semiJoinClazz = TupleBasedLeftSemiHashJoinOperator.class;
    boolean disablePTOptimizationTrick;

    private PlanBuildContext context;
    private FullReducerOperator fullReducerOperator;
    private final RuleType ruleType = RuleType.IN_PLACE;

    public AttachFullReducer()
    {
    }

    //FIXME: temporarily workaround for SSB benchmarking setup (BenchmarkSSB)
    public AttachFullReducer(Class<? extends TupleBasedJoinOperator> semiJoinClazz)
    {
        this.semiJoinClazz = semiJoinClazz;
    }

    @Override
    public Plan applyToLogicalPlan(Plan plan, PlanBuildContext context)
    {
        this.context = requireNonNull(context, "context is null");
        PlanNode root = plan.getRoot();
        List<PlanNode> tableNodes = gatherTableScanNodes(root);
        FullReducerNode fullReducerNode = new FullReducerNode(context.getPlanNodeIdAllocator().getNextId(), tableNodes);
        fullReducerNode.setSink(root);
        return new Plan(fullReducerNode);
    }

    @Override
    public Plan applyToPhysicalPlan(PlanNode node, RuleContext context)
    {
        RuleStatistics.Builder builder = RuleStatistics.builder(this.getClass());
        fullReducerOperator = (FullReducerOperator) context.getOperator();
        if (context.getPlanBuildContext().getSemiJoinClazz() != null) {
            this.semiJoinClazz = context.getPlanBuildContext().getSemiJoinClazz();
        }
        else {
            // For testing purpose only
            context.getPlanBuildContext().setSemiJoinClazz(this.semiJoinClazz);
        }
        updateRuleConfig(context.getPlanBuildContext());
        List<PlanNode> tableNodes = gatherTableScanNodes(node);
        Map<Integer, List<Operator>> node2Operators = new HashMap<>();
        for (PlanNode tableNode : tableNodes) {
            node2Operators.computeIfAbsent(tableNode.getOperator().getMultiwayJoinNode().getNodeId(), k -> new ArrayList<>());
            node2Operators.get(tableNode.getOperator().getMultiwayJoinNode().getNodeId()).add(tableNode.getOperator());
        }
        SemiJoinOrdering semiJoinOrdering = context.getPlanBuildContext().getSemiJoinOrdering();
        MultiwayJoinOrderedGraph orderedGraph = this.context.getOrderedGraph();
        if (this.context.getEnableJoinGraphHeuristicFromPT()) {
            orderedGraph = joinGraphHeuristicsFromPT();
            fullReducerOperator.setSemijoins(generateSemiJoinsFromDAG(orderedGraph, true, node2Operators), true);
            fullReducerOperator.setSemijoins(generateSemiJoinsFromDAG(orderedGraph, false, node2Operators), false);
        }
        else {
            if (semiJoinOrdering == null) {
                if (disablePTOptimizationTrick) {
                    fullReducerOperator.setSemijoins(generateFullReducerVanilla(orderedGraph, true, node2Operators), true);
                }
                else {
                    fullReducerOperator.setSemijoins(generateFullReducer(orderedGraph, true, node2Operators), true);
                }
            }
            else {
                checkState(orderedGraph == semiJoinOrdering.getJoinTree(),
                        "orderedGraph in PlanBuildContext is not the same reference as join tree in SemijoinOrdering");
                if (disablePTOptimizationTrick) {
                    fullReducerOperator.setSemijoins(generateFullReducerVanilla(semiJoinOrdering.getBottomUpPass(), node2Operators), true);
                }
                else {
                    fullReducerOperator.setSemijoins(generateFullReducer(semiJoinOrdering.getBottomUpPass(), node2Operators), true);
                }
            }
            if (disablePTOptimizationTrick) {
                fullReducerOperator.setSemijoins(generateFullReducerVanilla(orderedGraph, false, node2Operators), false);
            }
            else {
                fullReducerOperator.setSemijoins(generateFullReducer(orderedGraph, false, node2Operators), false);
            }
        }
        fullReducerOperator.setNode2Operators(node2Operators);
        ruleStatistics = builder.build();
        return null;
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        return node.getNodeType() == OptType.fullReducer;
    }

    @Override
    public RuleType getRuleType()
    {
        return ruleType;
    }

    /**
     * We create a list of semi-join operations based on the given orderedGraph.
     * The result is a hashmap: depending on whether we are generating
     * reducing semi-join program (i.e., the bottom-up pass of the join tree),
     * if we are generating a reducing semi-join program (i.e., toGenerateSemijoinReducingProgram = true),
     * each key in the hash map is a parent node, and the value is the list of plans representing
     * semi-join operations with the same parent node, i.e., parent \leftsemijoin child for all possible children
     * of the given parent. If toGenerateSemijoinReducingProgram = false, we are generating
     * the top-down pass of the join tree, i.e., child \leftsemijoin parent instead. Thus, each key in
     * the hashmap is the child node and the corresponding list<Plan> (with size = 1) is the semi-join operation.
     */
    private List<Plan> generateFullReducer(MultiwayJoinOrderedGraph orderedGraph, boolean toGenerateSemijoinReducingProgram, Map<Integer, List<Operator>> node2Operators)
    {
        List<Plan> semijoins = new ArrayList<>();
        LinkedHashMap<Integer, List<MultiwayJoinNode>> depth = orderedGraph.getDepth();
        int height = depth.size();
        if (toGenerateSemijoinReducingProgram) {
            for (int i = height - 1; i >= 1; i--) {
                List<MultiwayJoinNode> childrenBaseNodes = depth.get(i);
                HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> parent2Children = new HashMap<>();
                for (MultiwayJoinNode multiwayJoinChildNode : childrenBaseNodes) {
                    List<MultiwayJoinNode> parentBaseNodes = orderedGraph.getParent().get(multiwayJoinChildNode);
                    for (MultiwayJoinNode parent : parentBaseNodes) {
                        parent2Children.computeIfAbsent(parent, k -> new ArrayList<>());
                        parent2Children.get(parent).add(multiwayJoinChildNode);
                    }
                }
                for (MultiwayJoinNode parent : parent2Children.keySet()) {
                    semijoins.add(createPlanFromMultiwayJoinNodes(parent, parent2Children.get(parent)));
                    mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
                }
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                List<MultiwayJoinNode> childrenBaseNodes = depth.get(i);
                for (MultiwayJoinNode multiwayJoinChildNode : childrenBaseNodes) {
                    List<MultiwayJoinNode> parents = orderedGraph.getChildren().get(multiwayJoinChildNode);
                    for (MultiwayJoinNode multiwayJoinParentNode : parents) {
                        semijoins.add(createPlanFromMultiwayJoinNodes(multiwayJoinParentNode, List.of(multiwayJoinChildNode)));
                        mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
                    }
                }
            }
        }
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("toGenerateSemijoinReducingProgram: " + toGenerateSemijoinReducingProgram);
            semijoins.forEach(plan -> {
                PlanPrinter printer = new PlanPrinter(plan.getRoot());
                traceLogger.debug(printer.toText(0));
            });
        }
        return semijoins;
    }

    /**
     * Generate Full Reducer but without PT optimization trick: for a node S and all its children, instead of performing binary-wise semijoin,
     * we can build a left-deep query plan where S is the left-most relation and all its children are R_{inner} relations. Doing so,
     * we reduce the number of materalization to S to 1 regardless the number of children that S has.
     */
    private List<Plan> generateFullReducerVanilla(MultiwayJoinOrderedGraph orderedGraph, boolean toGenerateSemijoinReducingProgram, Map<Integer, List<Operator>> node2Operators)
    {
        List<Plan> semijoins = new ArrayList<>();
        LinkedHashMap<Integer, List<MultiwayJoinNode>> depth = orderedGraph.getDepth();
        int height = depth.size();
        if (toGenerateSemijoinReducingProgram) {
            for (int i = height - 1; i >= 1; i--) {
                List<MultiwayJoinNode> childrenBaseNodes = depth.get(i);
                for (MultiwayJoinNode multiwayJoinChildNode : childrenBaseNodes) {
                    List<MultiwayJoinNode> parentBaseNodes = orderedGraph.getParent().get(multiwayJoinChildNode);
                    for (MultiwayJoinNode multiwayJoinParentNode : parentBaseNodes) {
                        semijoins.add(createPlanFromMultiwayJoinNodes(multiwayJoinParentNode, List.of(multiwayJoinChildNode)));
                        mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
                    }
                }
            }
        }
        else {
            for (int i = 0; i < height; i++) {
                List<MultiwayJoinNode> childrenBaseNodes = depth.get(i);
                for (MultiwayJoinNode multiwayJoinChildNode : childrenBaseNodes) {
                    List<MultiwayJoinNode> parents = orderedGraph.getChildren().get(multiwayJoinChildNode);
                    for (MultiwayJoinNode multiwayJoinParentNode : parents) {
                        semijoins.add(createPlanFromMultiwayJoinNodes(multiwayJoinParentNode, List.of(multiwayJoinChildNode)));
                        mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
                    }
                }
            }
        }
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("toGenerateSemijoinReducingProgram: " + toGenerateSemijoinReducingProgram);
            semijoins.forEach(plan -> {
                PlanPrinter printer = new PlanPrinter(plan.getRoot());
                traceLogger.debug(printer.toText(0));
            });
        }
        return semijoins;
    }

    private List<Plan> generateFullReducer(List<Pair<MultiwayJoinNode, MultiwayJoinNode>> pass, Map<Integer, List<Operator>> node2Operators)
    {
        List<Plan> semijoins = new ArrayList<>();
        MultiwayJoinNode prevParent = null;
        List<MultiwayJoinNode> children = new ArrayList<>();
        for (Pair<MultiwayJoinNode, MultiwayJoinNode> pair : pass) {
            MultiwayJoinNode parent = pair.getKey();
            if (prevParent == null) {
                prevParent = parent;
            }
            if (parent == prevParent) {
                children.add(pair.getValue());
            }
            else {
                semijoins.add(createPlanFromMultiwayJoinNodes(prevParent, children));
                mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
                prevParent = parent;
                children.clear();
                children.add(pair.getValue());
            }
        }
        semijoins.add(createPlanFromMultiwayJoinNodes(prevParent, children));
        mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
        return semijoins;
    }

    private List<Plan> generateFullReducerVanilla(List<Pair<MultiwayJoinNode, MultiwayJoinNode>> pass, Map<Integer, List<Operator>> node2Operators)
    {
        List<Plan> semijoins = new ArrayList<>();
        for (Pair<MultiwayJoinNode, MultiwayJoinNode> pair : pass) {
            semijoins.add(createPlanFromMultiwayJoinNodes(pair.getKey(), List.of(pair.getValue())));
            mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
        }
        return semijoins;
    }

    private Plan createPlanFromMultiwayJoinNodes(MultiwayJoinNode parent, List<MultiwayJoinNode> chidren)
    {
        MultiwayJoinOrderedGraph orderedGraph = constructOrderedGraph(parent, chidren);
        PlanBuildContext context = PlanBuildContext.builder()
                .setOrderedGraph(orderedGraph)
                .setRules(Collections.emptyList())
                .setPlanNodeIdAllocator(this.context.getPlanNodeIdAllocator())
                .setJdbcClient(this.context.getJdbcClient())
                .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class), Optional.of(semiJoinClazz)))
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        return physicalPlanBuilder.build(logicalPlan.getRoot());
    }

    public static MultiwayJoinOrderedGraph constructOrderedGraph(MultiwayJoinNode parent, List<MultiwayJoinNode> children)
    {
        List<MultiwayJoinNode> traversalList = new ArrayList<>(List.of(parent));
        traversalList.addAll(children);
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> childrenMap = new HashMap<>();
        childrenMap.put(parent, children);
        for (MultiwayJoinNode child : children) {
            childrenMap.put(child, new ArrayList<>());
        }
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> parentMap = new HashMap<>();
        for (MultiwayJoinNode child : children) {
            parentMap.put(child, List.of(parent));
        }
        parentMap.put(parent, new ArrayList<>());
        return new MultiwayJoinOrderedGraph(parent, childrenMap, parentMap, traversalList);
    }

    /**
     * We gather all table scan nodes in the existing (sub) plan for the given root.
     */
    public static class TableScanVisitor
            extends PlanVisitor<Void, Void>
    {
        private final List<PlanNode> tableScanNodes;

        public static List<PlanNode> gatherTableScanNodes(PlanNode root)
        {
            return (new TableScanVisitor(root)).tableScanNodes;
        }

        private TableScanVisitor(PlanNode root)
        {
            tableScanNodes = new ArrayList<>();
            visitPlan(root, null);
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
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
            tableScanNodes.add(node);
            return null;
        }

        @Override
        public Void visitFullReducer(FullReducerNode node, Void context)
        {
            return null;
        }
    }

    private void mergeNode2Operators(Plan plan, Map<Integer, List<Operator>> node2Operators)
    {
        Map<Integer, List<Operator>> planNode2Operators = plan.getNode2Operators();
        for (Integer nodeId : planNode2Operators.keySet()) {
            node2Operators.computeIfAbsent(nodeId, k -> new ArrayList<>());
            node2Operators.get(nodeId).addAll(planNode2Operators.get(nodeId));
        }
    }

    private void updateRuleConfig(PlanBuildContext context)
    {
        if (context.getSemiJoinClazz() != null) {
            this.semiJoinClazz = context.getSemiJoinClazz();
        }
        if (context.getDisablePTOptimizationTrick() != null) {
            this.disablePTOptimizationTrick = context.getDisablePTOptimizationTrick();
        }
    }

    /**
     * Forward pass corresponds to bottom-up pass in YA.
     */
    private List<Plan> generateSemiJoinsFromDAG(MultiwayJoinOrderedGraph orderedGraph, boolean isForwardPass, Map<Integer, List<Operator>> node2Operators)
    {
        List<Plan> semijoins = new ArrayList<>();
        List<MultiwayJoinNode> topologicalSort = orderedGraph.topologicalSort();
        if (isForwardPass) {
            for (int i = 0; i < topologicalSort.size() - 1; i++) {
                MultiwayJoinNode node = topologicalSort.get(i);
                List<MultiwayJoinNode> children = orderedGraph.getChildren().get(node);
                if (!children.isEmpty()) {
                    semijoins.add(createPlanFromMultiwayJoinNodes(node, children));
                }
                mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
            }
            Collections.reverse(semijoins);
            return semijoins;
        }
        else {
            for (int i = topologicalSort.size() - 1; i >= 1; i--) {
                MultiwayJoinNode node = topologicalSort.get(i);
                List<MultiwayJoinNode> parents = orderedGraph.getParent().get(node);
                if (!parents.isEmpty()) {
                    semijoins.add(createPlanFromMultiwayJoinNodes(node, parents));
                }
                mergeNode2Operators(semijoins.get(semijoins.size() - 1), node2Operators);
            }
            Collections.reverse(semijoins);
            return semijoins;
        }
    }

    private MultiwayJoinOrderedGraph joinGraphHeuristicsFromPT()
    {
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> children = new HashMap<>();
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> parent = new HashMap<>();
        MultiwayJoinGraph joinGraph = context.getOrderedGraph().obtainQueryGraph();
        List<MultiwayJoinNode> nodes = new ArrayList<>(joinGraph.getNodes());
        for (MultiwayJoinNode node : nodes) {
            node.setNodeType(NodeType.None);
            List<MultiwayJoinNode> connected = node.getConnected();
            for (MultiwayJoinNode neighbor : connected) {
                neighbor.setNodeType(NodeType.None);
                // In PT, smaller table is child and larger table is parent, i.e., smaller -> larger, which indicates the forward
                // pass of the filter. In our implementation, the direction is flipped: larger -> smaller so that the forward pass
                // will still from smaller to larger
                long nodeSize = this.context.getCatalogGroup().getTableCatalog(node.getSchemaTableName()).getSize();
                long neighborSize = this.context.getCatalogGroup().getTableCatalog(neighbor.getSchemaTableName()).getSize();
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(String.format("%s: %s", node, nodeSize));
                    traceLogger.trace(String.format("%s: %s", neighbor, neighborSize));
                }
                if (nodeSize <= neighborSize) {
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(String.format("%s -> %s", neighbor, node));
                    }
                    children.computeIfAbsent(neighbor, k -> new ArrayList<>());
                    if (!children.get(neighbor).contains(node)) {
                        children.get(neighbor).add(node);
                    }
                    children.computeIfAbsent(node, k -> new ArrayList<>());
                    parent.computeIfAbsent(node, k -> new ArrayList<>());
                    if (!parent.get(node).contains(neighbor)) {
                        parent.get(node).add(neighbor);
                    }
                    parent.computeIfAbsent(neighbor, k -> new ArrayList<>());
                }
                else {
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(String.format("%s -> %s", node, neighbor));
                    }
                    children.computeIfAbsent(node, k -> new ArrayList<>());
                    if (!children.get(node).contains(neighbor)) {
                        children.get(node).add(neighbor);
                    }
                    parent.computeIfAbsent(node, k -> new ArrayList<>());
                    parent.computeIfAbsent(neighbor, k -> new ArrayList<>());
                    if (!parent.get(neighbor).contains(node)) {
                        parent.get(neighbor).add(node);
                    }
                    children.computeIfAbsent(neighbor, k -> new ArrayList<>());
                }
            }
        }
        List<MultiwayJoinNode> roots = new ArrayList<>();
        for (MultiwayJoinNode child : children.keySet()) {
            if (children.get(child).isEmpty()) {
                // In DAG, Leaf is sink
                child.setNodeType(NodeType.Leaf);
            }
        }
        for (MultiwayJoinNode parentNode : parent.keySet()) {
            if (parent.get(parentNode).size() == 0) {
                // In DAG, Leaf is source
                parentNode.setNodeType(NodeType.Root);
                roots.add(parentNode);
            }
        }
        for (MultiwayJoinNode node : nodes) {
            if (node.getNodeType() == NodeType.None) {
                node.setNodeType(NodeType.Internal);
            }
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace("there are " + roots.size() + "roots");
        }
        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(roots.get(0), children, parent, nodes);
        checkState(orderedGraph.isOgADag(), "orderedGraph is not a DAG");
        return orderedGraph;
    }
}
