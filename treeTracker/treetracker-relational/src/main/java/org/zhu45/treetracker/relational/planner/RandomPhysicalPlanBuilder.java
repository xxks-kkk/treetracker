package org.zhu45.treetracker.relational.planner;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcRecordSetProvider;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.RecordIntTupleSourceProvider;
import org.zhu45.treetracker.jdbc.RecordObjectTupleSourceProvider;
import org.zhu45.treetracker.jdbc.RecordTupleSourceProvider;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.FullReducerOperator;
import org.zhu45.treetracker.relational.operator.JoinType;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TreeTrackerBFJoinOperator;
import org.zhu45.treetracker.relational.operator.TreeTrackerJoinOperator;
import org.zhu45.treetracker.relational.operator.TreeTrackerJoinV2Operator;
import org.zhu45.treetracker.relational.operator.TreeTrackerTableScanOperator;
import org.zhu45.treetracker.relational.operator.TreeTrackerTableScanV2Operator;
import org.zhu45.treetracker.relational.operator.TupleBaseTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBaseTreeTrackerOneBetaTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLIPHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftAntiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiBloomJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinIntOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedNestedLoopJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedSSBLIPHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.operator.noGoodList.DefaultIntRowNoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.DefaultNoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.SimpleNoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavIntRowNoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavNoGoodListMap;
import org.zhu45.treetracker.relational.planner.catalog.TableCatalog;
import org.zhu45.treetracker.relational.planner.plan.AggregateNode;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.GatherMergeNode;
import org.zhu45.treetracker.relational.planner.plan.GatherNode;
import org.zhu45.treetracker.relational.planner.plan.HashNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.MaterializeNode;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.plan.Side;
import org.zhu45.treetracker.relational.planner.plan.SortNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.rule.Rule;
import org.zhu45.treetracker.relational.planner.rule.RuleContext;
import org.zhu45.treetracker.relational.planner.rule.RuleType;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.internal.util.Preconditions.checkArgument;
import static com.google.inject.internal.util.Preconditions.checkState;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;
import static org.zhu45.treetracker.relational.operator.noGoodList.DefaultIntRowNoGoodListMap.constructDefaultIntRowNoGoodListMap;
import static org.zhu45.treetracker.relational.operator.noGoodList.DefaultNoGoodListMap.constructDefaultNoGoodListMap;
import static org.zhu45.treetracker.relational.operator.noGoodList.SimpleNoGoodListMap.constructSimpleNoGoodListMap;
import static org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavIntRowNoGoodListMap.constructSingleValueJavIntRowNoGoodListMap;
import static org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavNoGoodListMap.constructSingleValueJavNoGoodListMap;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.AttachPlanBuildContextVisitor.attachPlanBuildContext;
import static org.zhu45.treetracker.relational.planner.rule.DecideBackJumpingPointsForBushyPlan.initializeContextObject;

public class RandomPhysicalPlanBuilder
        extends PlanVisitor<PlanNode, RandomPhysicalPlanBuilder.PlanVisitorContext>
{
    private static final Logger traceLogger;
    private PlanStatistics planStatistics;
    private Map<Integer, List<Operator>> node2Operators = new HashMap<>();
    private List<TableNode> outerTables = new ArrayList<>();

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(RandomPhysicalPlanBuilder.class);
        }
        else {
            traceLogger = null;
        }
    }

    private final Map<OptType, List<Class<? extends Operator>>> operatorMap;
    public static Map<OptType, List<Class<? extends Operator>>> defaultOperatorMap = createMap(Optional.empty(), Optional.empty());

    // !IMPORTANT! Any new operator implementation should be appended at the end of each list
    // (i.e., replacing 1st element with different operator class will result in
    // testPrintPhysicalPlanToText test failure)
    public static Map<OptType, List<Class<? extends Operator>>> createMap(Optional<Class<? extends TupleBasedTableScanOperator>> tableScanOperatorClazz,
                                                                          Optional<Class<? extends TupleBasedJoinOperator>> joinOperatorClazz)
    {
        Map<OptType, List<Class<? extends Operator>>> myMap = new HashMap<>();
        tableScanOperatorClazz.ifPresentOrElse(tableScanClazz -> myMap.put(OptType.table, Collections.singletonList(tableScanClazz)),
                () -> myMap.put(OptType.table, Collections.singletonList(TupleBasedTableScanOperator.class)));
        joinOperatorClazz.ifPresentOrElse(joinClazz -> myMap.put(OptType.join, Collections.singletonList(joinClazz)),
                () -> myMap.put(OptType.join, Collections.singletonList(TupleBasedNestedLoopJoinOperator.class)));
        myMap.put(OptType.fullReducer, Collections.singletonList(FullReducerOperator.class));
        return myMap;
    }

    public static class PlanVisitorContext
    {
        Map<OptType, List<Class<? extends Operator>>> operatorMap;
        int traceDepth;

        public PlanVisitorContext(Map<OptType, List<Class<? extends Operator>>> operatorMap, PlanNode parentNode, int traceDepth)
        {
            this.operatorMap = operatorMap;
            this.traceDepth = traceDepth;
        }

        void setTraceDepth(int traceDepth)
        {
            this.traceDepth = traceDepth;
        }
    }

    private final JdbcClient jdbcClient;
    private PlanBuildContext context;

    public RandomPhysicalPlanBuilder(PlanBuildContext context)
    {
        this.jdbcClient = requireNonNull(context.getJdbcClient(), "JdbcClient is null");
        this.context = context;
        this.operatorMap = context.getOperatorMap();
        context.initializeCatalogGroup(context.getSchemaTableNameList());
        planStatistics = new PlanStatistics();
    }

    public Plan build(PlanNode logicalPlanRoot)
    {
        Plan phyiscalPlan = new Plan(visitPlan(logicalPlanRoot, new PlanVisitorContext(operatorMap, null, 1)), planStatistics);
        phyiscalPlan.setNode2Operators(node2Operators);
        context.setJdbcClient(jdbcClient);
        attachPlanBuildContext(phyiscalPlan.getRoot(), context);
        // Apply AS_A_WHOLE optimization rule to the physical plan
        for (Rule rule : context.getRules()) {
            if (rule.getRuleType() == RuleType.AS_A_WHOLE && rule.checkForRulePrecondition(phyiscalPlan.getRoot())) {
                // We create ruleContext each time we apply rule because rule may create a new plan and we want to
                // keep all the existing PlanStatistics.
                RuleContext ruleContext = RuleContext.builder().setPlanStatistics(phyiscalPlan.getPlanStatistics()).setJdbcClinet(context.getJdbcClient())
                        .setPlanNodeIdAllocator(context.getPlanNodeIdAllocator()).setOperatorMap(context.getOperatorMap()).setPlanBuildContext(context)
                        .setPlan(phyiscalPlan)
                        .build();
                phyiscalPlan = rule.applyToPhysicalPlan(phyiscalPlan.getRoot(), ruleContext);
                phyiscalPlan.getPlanStatistics().mergeRuleStatistics(rule.getRuleStatistics());
                context = phyiscalPlan.getRoot().getOperator().getPlanBuildContext();
            }
        }
        setNoGoodListMap(phyiscalPlan);
        // FIXME: Below is a hack to ensure planStatistics NoGoodListMapClazz is not overriden by the rule
        //  application. The correct way is that rule should ensure planStatistics is properly preserved (e.g.,
        //  createPhysicalPlanFromJoinOrdering call in FindTheBestJoinOrderingJoinTreeWithDP doesn't preserve
        //  planStatistics, which should be fixed)
        if (outerTables.size() == 1 && outerTables.get(0).getOperator().getNoGoodListMap() != null) {
            phyiscalPlan.getPlanStatistics().setNoGoodListMapClazz(outerTables.get(0).getOperator().getNoGoodListMap().getClass().getCanonicalName());
        }
        if (ifUseTTJFamilyOperator(context.getOperatorMap(), true)) {
            initializeContextObject(phyiscalPlan.getRoot());
        }
        if (ifUseLIPFamilyOperator(context.getOperatorMap())) {
            initializeFactorTableOperator(phyiscalPlan.getRoot());
        }
        return phyiscalPlan;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        List<PlanNode> children = node.getSources();
        for (PlanNode child : children) {
            if (child != null) {
                planVisitorContext.setTraceDepth(planVisitorContext.traceDepth + 1);
                visitPlan(child, planVisitorContext);
            }
        }
        return node.accept(this, planVisitorContext);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        visitNode(node, planVisitorContext);
        setOperatorAssociatedRelationSizeForJoin(node);
        TupleBasedJoinOperator joinOperator = (TupleBasedJoinOperator) node.getOperator();
        if (planVisitorContext.operatorMap.get(OptType.join).contains(TupleBasedLeftSemiHashJoinOperator.class) ||
                planVisitorContext.operatorMap.get(OptType.join).contains(TupleBasedLeftSemiBloomJoinOperator.class) ||
                planVisitorContext.operatorMap.get(OptType.join).contains(TupleBasedLeftSemiHashJoinIntOperator.class)) {
            joinOperator.construct(JoinType.LeftSemiJoin);
        }
        else if (planVisitorContext.operatorMap.get(OptType.join).contains(TupleBasedLeftAntiHashJoinOperator.class)) {
            joinOperator.construct(JoinType.LeftAntiJoin);
        }
        else {
            joinOperator.construct(JoinType.NaturalJoin);
        }
        return node;
    }

    private void setOperatorAssociatedRelationSizeForJoin(JoinNode node)
    {
        checkState(node.getLeft().getSide() == Side.OUTER, "left child should be Side.OUTER");
        checkState(node.getRight().getSide() == Side.INNER, "right child should be Side.INNER");
        Operator operator = null;
        if (node.getRight().getNodeType() == OptType.hash
                || node.getRight().getNodeType() == OptType.gather
                || node.getRight().getNodeType() == OptType.sort
                || node.getRight().getNodeType() == OptType.materialize
                || node.getRight().getNodeType() == OptType.gather_merge
                || node.getRight().getNodeType() == OptType.aggregate) {
            PlanNode targetNode = node.getRight().getSources().get(0);
            operator = targetNode.getOperator();
            while (operator == null) {
                checkState(targetNode.getSources().size() == 1, targetNode + " should have only one child");
                targetNode = targetNode.getSources().get(0);
                operator = targetNode.getOperator();
            }
        }
        else {
            operator = node.getRight().getOperator();
        }
        if (operator.getOperatorType() == OptType.table) {
            node.getOperator().setOperatorAssociatedRelationSize(operator.getOperatorAssociatedRelationSize());
        }
        else {
            // This branch can only happen when we use Postgres plan. In Postgres plan, it contains "Plan Rows", which
            // provides intermediate results estimates and "Actual Rows", which precisely the intermediate result
            // size if the plan is generated from "EXPLAIN ANALYZE".
            node.getOperator().setOperatorAssociatedRelationSize(max(node.getRight().getActualRows(), node.getRight().getPlanRows()));
        }
    }

    @Override
    public PlanNode visitHash(HashNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        checkArgument(node.getSources().size() == 1, "HashNode should have only one child");
        return node.getSources().get(0);
    }

    @Override
    public PlanNode visitGather(GatherNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        checkArgument(node.getSources().size() == 1, "GatherNode should have only one child");
        return node.getSources().get(0);
    }

    @Override
    public PlanNode visitGatherMerge(GatherMergeNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        checkArgument(node.getSources().size() == 1, "GatherNode should have only one child");
        return node.getSources().get(0);
    }

    @Override
    public PlanNode visitAggregate(AggregateNode node, PlanVisitorContext context)
    {
        checkArgument(node.getSources().size() == 1, "AggregateNode should have only one child");
        return node.getSources().get(0);
    }

    @Override
    public PlanNode visitSort(SortNode node, PlanVisitorContext context)
    {
        checkArgument(node.getSources().size() == 1, "SortNode should have only one child");
        return node.getSources().get(0);
    }

    @Override
    public PlanNode visitMaterialize(MaterializeNode node, PlanVisitorContext context)
    {
        checkArgument(node.getSources().size() == 1, "MaterializeNode should have only one child");
        return node.getSources().get(0);
    }

    @Override
    public PlanNode visitTable(TableNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        node.setJdbcClient(jdbcClient);
        visitNode(node, planVisitorContext);
        return node;
    }

    @Override
    public PlanNode visitFullReducer(FullReducerNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        visitNode(node, planVisitorContext);
        return node;
    }

    private <T extends Operator> void visitNode(PlanNode node, RandomPhysicalPlanBuilder.PlanVisitorContext planVisitorContext)
    {
        List<Class<? extends Operator>> operatorClazzList = planVisitorContext.operatorMap.get(node.getNodeType());
        requireNonNull(operatorClazzList, "operatorMap contains no operator implementation class for the given node type " + node.getNodeType());
        checkArgument(operatorClazzList.size() == 1, "operatorClazzList should contain only one operator");
        Class<? extends Operator> operatorClazz = operatorClazzList.get(0);
        try {
            Constructor<? extends Operator> constructor = operatorClazz.getConstructor();
            T operator = null;
            switch (node.getNodeType()) {
                case fullReducer:
                case join:
                    operator = (T) constructor.newInstance();
                    break;
                case table:
                    TableNode tableNode = (TableNode) node;
                    operator = createTableScanOperator(tableNode, constructor, context, node2Operators);
                    break;
                default:
                    break;
            }
            operator.setOperatorID(node.getId());
            operator.setSide(node.getSide());
            operator.setOperatorTraceDepth(planVisitorContext.traceDepth);
            node.setOperator(operator);
            // Apply IN_PLACE optimization rule to physical plan
            for (Rule rule : context.getRules()) {
                if (rule.getRuleType() == RuleType.IN_PLACE && rule.checkForRulePrecondition(node)) {
                    RuleContext ruleContext = RuleContext.builder().setOperator(operator).setPlanBuildContext(context).build();
                    rule.applyToPhysicalPlan(node, ruleContext);
                    // NOTE: maybe we have a risk of overriding field of RuleStatistics
                    // when merging but given current fields of RuleStatistics (mostly related to AS_A_WHOLE rule)
                    // we don't have this issue but should aware.
                    planStatistics.mergeRuleStatistics(rule.getRuleStatistics());
                }
            }
        }
        catch (NoSuchMethodException e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, String.format("There is no constructor found for %s", operatorClazz.getName()));
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
        }
    }

    public static <T extends Operator> T createTableScanOperator(TableNode tableNode, Constructor<? extends Operator> constructor, PlanBuildContext context,
                                                                 Map<Integer, List<Operator>> node2Operators)
    {
        T operator = null;
        try {
            if (tableNode.getSide() == Side.OUTER) {
                operator = (T) constructor.newInstance();
            }
            else {
                // By default, i.e., non R_k node, we use normal table scan operator. The specified table scan operator
                // in operatorMap only impacts the table scan implementation of R_k.
                operator = (T) TupleBasedTableScanOperator.class.getConstructor().newInstance();
            }
            if (tableNode.getMultiwayJoinNode() != null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace("tableNode.getMultiwayJoinNode: " + tableNode.getMultiwayJoinNode());
                }
                node2Operators.computeIfAbsent(tableNode.getMultiwayJoinNode().getNodeId(), k -> new ArrayList<>());
                operator.setMultiwayJoinNode(tableNode.getMultiwayJoinNode());
                node2Operators.get(tableNode.getMultiwayJoinNode().getNodeId()).add(operator);
            }
            operator.setSchemaTableName(tableNode.getSchemaTableName());
            setOperatorAssociatedRelationSize(operator, context.getJdbcClient());
            operator.setRecordTupleSourceProvider(decideRecordTupleSourceProviderImpl(operator.getSchemaTableName(), context));
            operator.setTableCatalog(context.getCatalogGroup().getTableCatalog(operator.getSchemaTableName()));
            operator.setSide(tableNode.getSide());
            operator.setPlanBuildContext(context);
            return operator;
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
        }
    }

    private static void setOperatorAssociatedRelationSize(Operator operator, JdbcClient jdbcClient)
    {
        checkArgument(operator.getOperatorType() == OptType.table);
        try (Connection connection = jdbcClient.getConnection()) {
            SchemaTableName schemaTableName = operator.getSchemaTableName();
            JdbcTableHandle jdbcTableHandle = jdbcClient.getTableHandle(schemaTableName);
            operator.setOperatorAssociatedRelationSize(jdbcClient.getTableSize(connection, jdbcTableHandle));
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    /**
     * This class binds PlanBuildContext to the operator so that information
     * in PlanBuildContext is accessible to the operator during the evaluation.
     */
    public static class AttachPlanBuildContextVisitor
            extends PlanVisitor<Void, PlanBuildContext>
    {
        public static void attachPlanBuildContext(PlanNode root, PlanBuildContext context)
        {
            new AttachPlanBuildContextVisitor(root, context);
        }

        private AttachPlanBuildContextVisitor(PlanNode root, PlanBuildContext context)
        {
            visitPlan(root, context);
        }

        @Override
        public Void visitPlan(PlanNode node, PlanBuildContext context)
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
        public Void visitJoin(JoinNode node, PlanBuildContext context)
        {
            Operator operator = node.getOperator();
            operator.setPlanBuildContext(context);
            return null;
        }

        @Override
        public Void visitHash(HashNode node, PlanBuildContext context)
        {
            return null;
        }

        @Override
        public Void visitGather(GatherNode node, PlanBuildContext context)
        {
            return null;
        }

        @Override
        public Void visitGatherMerge(GatherMergeNode node, PlanBuildContext context)
        {
            return null;
        }

        @Override
        public Void visitAggregate(AggregateNode node, PlanBuildContext context)
        {
            return null;
        }

        @Override
        public Void visitSort(SortNode node, PlanBuildContext context)
        {
            return null;
        }

        @Override
        public Void visitMaterialize(MaterializeNode node, PlanBuildContext context)
        {
            return null;
        }

        @Override
        public Void visitTable(TableNode node, PlanBuildContext context)
        {
            Operator operator = node.getOperator();
            operator.setPlanBuildContext(context);
            return null;
        }

        @Override
        public Void visitFullReducer(FullReducerNode node, PlanBuildContext context)
        {
            Operator operator = node.getOperator();
            operator.setPlanBuildContext(context);
            return null;
        }
    }

    private NoGoodListMap decideNoGoodListMapImpl(TableNode outerTable, Plan physicalPlan, PlanBuildContext context)
    {
        PlanNode root = physicalPlan.getRoot();
        checkArgument(root.getOperator().getOperatorType() == OptType.join);
        Class<? extends Operator> operatorClass = root.getOperator().getClass();
        if (operatorClass == TupleBaseTreeTrackerOneBetaHashTableOperator.class) {
            physicalPlan.getPlanStatistics().setNoGoodListMapClazz(SimpleNoGoodListMap.class.getCanonicalName());
            return constructSimpleNoGoodListMap(outerTable, context);
        }
        Pair<Boolean, SingleValueJavIntRowNoGoodListMap> pair = constructSingleValueJavIntRowNoGoodListMap(outerTable, root, context);
        if (outerTable.getOperator().getRecordTupleSourceProviderClazz().equals(RecordIntTupleSourceProvider.class) && pair.getKey()) {
            // FIXME: We assume all the no-good list will have the same class, i.e., there doesn't exist that we mix-and-match
            //  different no-good list map implementation in the same query. If so, we should have List<String> instead of String NoGoodListMapClazz
            //  in PlanStatistics.
            physicalPlan.getPlanStatistics().setNoGoodListMapClazz(SingleValueJavIntRowNoGoodListMap.class.getCanonicalName());
            return pair.getRight();
        }
        Pair<Boolean, SingleValueJavNoGoodListMap> pair2 = constructSingleValueJavNoGoodListMap(outerTable, root, context);
        if (pair2.getKey()) {
            physicalPlan.getPlanStatistics().setNoGoodListMapClazz(SingleValueJavNoGoodListMap.class.getCanonicalName());
            return pair2.getRight();
        }
        if (outerTable.getOperator().getRecordTupleSourceProviderClazz().equals(RecordIntTupleSourceProvider.class)) {
            physicalPlan.getPlanStatistics().setNoGoodListMapClazz(DefaultIntRowNoGoodListMap.class.getCanonicalName());
            return constructDefaultIntRowNoGoodListMap(outerTable, context);
        }
        physicalPlan.getPlanStatistics().setNoGoodListMapClazz(DefaultNoGoodListMap.class.getCanonicalName());
        return constructDefaultNoGoodListMap(outerTable, context);
    }

    private void setNoGoodListMap(Plan plan)
    {
        gatherOuterTables(plan.getRoot(), outerTables);
        for (TableNode outerTable : outerTables) {
            if (isTTJScan(outerTable.getOperator().getClass()) &&
                    outerTable.getOperator().getNoGoodListMap() == null &&
                    ifUseTTJFamilyOperator(operatorMap, false)) {
                outerTable.getOperator().setNoGoodListMap(decideNoGoodListMapImpl(outerTable, plan, outerTable.getOperator().getPlanBuildContext()));
            }
        }
    }

    public static void gatherOuterTables(PlanNode root, List<TableNode> outerTables)
    {
        switch (root.getNodeType()) {
            case gather:
            case aggregate:
            case sort:
            case hash:
            case gather_merge:
            case materialize:
            case join:
            case fullReducer:
                for (PlanNode node : root.getSources()) {
                    gatherOuterTables(node, outerTables);
                }
                break;
            case table:
                TableNode tableNode = (TableNode) root;
                if (tableNode.getSide() == Side.OUTER) {
                    outerTables.add(tableNode);
                }
                break;
        }
    }

    private static RecordTupleSourceProvider decideRecordTupleSourceProviderImpl(SchemaTableName schemaTableName, PlanBuildContext context)
    {
        TableCatalog tableCatalog = context.getCatalogGroup().getTableCatalog(schemaTableName);
        if (tableCatalog.getTypeList().stream().allMatch(type -> type.equals(INTEGER))) {
            return new RecordIntTupleSourceProvider(new JdbcRecordSetProvider(context.getJdbcClient()));
        }
        return new RecordObjectTupleSourceProvider(new JdbcRecordSetProvider(context.getJdbcClient()));
    }

    public static boolean ifUseTTJFamilyOperator(Map<OptType, List<Class<? extends Operator>>> operatorMap, boolean ignoreTableScan)
    {
        if (!ignoreTableScan) {
            return (operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class) &&
                    operatorMap.get(OptType.table).contains(TupleBasedHighPerfTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class) &&
                            operatorMap.get(OptType.table).contains(TupleBasedHighPerfTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TreeTrackerJoinOperator.class) &&
                            operatorMap.get(OptType.table).contains(TreeTrackerTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TreeTrackerJoinV2Operator.class) &&
                            operatorMap.get(OptType.table).contains(TreeTrackerTableScanV2Operator.class)) ||
                    (operatorMap.get(OptType.join).contains(TupleBaseTreeTrackerOneBetaHashTableOperator.class) ||
                            operatorMap.get(OptType.table).contains(TupleBaseTreeTrackerOneBetaTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TreeTrackerBFJoinOperator.class) &&
                            operatorMap.get(OptType.table).contains(TupleBasedHighPerfTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator.class) &&
                            operatorMap.get(OptType.table).contains(TupleBasedHighPerfTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator.class) &&
                            operatorMap.get(OptType.table).contains(TupleBasedHighPerfTableScanOperator.class));
        }
        else {
            return operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class) ||
                    operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class) ||
                    operatorMap.get(OptType.join).contains(TreeTrackerJoinOperator.class) ||
                    operatorMap.get(OptType.join).contains(TreeTrackerJoinV2Operator.class) ||
                    operatorMap.get(OptType.join).contains(TupleBaseTreeTrackerOneBetaHashTableOperator.class) ||
                    operatorMap.get(OptType.join).contains(TreeTrackerBFJoinOperator.class) ||
                    operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator.class) ||
                    operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator.class);
        }
    }

    private boolean isTTJScan(Class<? extends Operator> operatorClazz)
    {
        return operatorClazz == TupleBasedHighPerfTableScanOperator.class ||
                operatorClazz == TreeTrackerTableScanOperator.class ||
                operatorClazz == TreeTrackerTableScanV2Operator.class ||
                operatorClazz == TupleBaseTreeTrackerOneBetaTableScanOperator.class;
    }

    public static PlanNode getLeftMostNode(PlanNode root)
    {
        List<TableNode> outerTables = new ArrayList<>();
        gatherOuterTables(root, outerTables);
        return outerTables.get(0);
    }

    private static boolean ifUseLIPFamilyOperator(Map<OptType, List<Class<? extends Operator>>> operatorMap)
    {
        return operatorMap.get(OptType.join).contains(TupleBasedLIPHashJoinOperator.class) ||
                operatorMap.get(OptType.join).contains(TupleBasedSSBLIPHashJoinOperator.class);
    }

    private static void initializeFactorTableOperator(PlanNode root)
    {
        List<TableNode> outerTables = new ArrayList<>();
        gatherOuterTables(root, outerTables);
        checkState(outerTables.size() == 1, "LIP only works with left-deep plan");
        PlanNode currNode = root;
        while (currNode.getNodeType() == OptType.join) {
            currNode.getOperator().setFactTableOperator(outerTables.get(0).getOperator());
            currNode = ((JoinNode) currNode).getLeft();
        }
    }
}
