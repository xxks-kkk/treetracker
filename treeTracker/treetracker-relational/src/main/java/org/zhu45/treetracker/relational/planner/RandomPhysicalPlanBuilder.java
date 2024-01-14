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
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftAntiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiBloomJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedNestedLoopJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.operator.noGoodList.DefaultNoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.SimpleNoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavIntRowNoGoodListMap;
import org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavNoGoodListMap;
import org.zhu45.treetracker.relational.planner.catalog.TableCatalog;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
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
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;
import static org.zhu45.treetracker.relational.operator.noGoodList.DefaultNoGoodListMap.constructDefaultNoGoodListMap;
import static org.zhu45.treetracker.relational.operator.noGoodList.SimpleNoGoodListMap.constructSimpleNoGoodListMap;
import static org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavIntRowNoGoodListMap.constructSingleValueJavIntRowNoGoodListMap;
import static org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavNoGoodListMap.constructSingleValueJavNoGoodListMap;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.AttachPlanBuildContextVisitor.attachPlanBuildContext;

public class RandomPhysicalPlanBuilder
        extends PlanVisitor<PlanNode, Pair<Map<OptType, List<Class<? extends Operator>>>, Integer>>
{
    private static final Logger traceLogger;
    private PlanStatistics planStatistics;
    private Map<Integer, List<Operator>> node2Operators = new HashMap<>();

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
    public static Map<OptType, List<Class<? extends Operator>>> createMap(
            Optional<Class<? extends TupleBasedTableScanOperator>> tableScanOperatorClazz,
            Optional<Class<? extends TupleBasedJoinOperator>> joinOperatorClazz)
    {
        Map<OptType, List<Class<? extends Operator>>> myMap = new HashMap<>();
        tableScanOperatorClazz.ifPresentOrElse(
                tableScanClazz -> myMap.put(OptType.table, Collections.singletonList(tableScanClazz)),
                () -> myMap.put(OptType.table, Collections.singletonList(TupleBasedTableScanOperator.class)));
        joinOperatorClazz.ifPresentOrElse(
                joinClazz -> myMap.put(OptType.join, Collections.singletonList(joinClazz)),
                () -> myMap.put(OptType.join, Collections.singletonList(TupleBasedNestedLoopJoinOperator.class)));
        myMap.put(OptType.fullReducer, Collections.singletonList(FullReducerOperator.class));
        return myMap;
    }

    private final JdbcClient jdbcClient;
    private PlanBuildContext context;

    public RandomPhysicalPlanBuilder(PlanBuildContext context)
    {
        this.jdbcClient = requireNonNull(context.getJdbcClient(), "JdbcClient is null");
        this.context = context;
        this.operatorMap = context.getOperatorMap();
        context.initializeCatalogGroup(context.getSchemaTableNameList());
        if (context.getOrderedGraph() != null) {
            context.setNodeId2FactTableJoinAttributeIdx();
        }
        planStatistics = new PlanStatistics();
    }

    public Plan build(PlanNode logicalPlanRoot)
    {
        Plan phyiscalPlan = new Plan(visitPlan(logicalPlanRoot, Pair.of(operatorMap, 1)), planStatistics);
        phyiscalPlan.setNode2Operators(node2Operators);
        context.setLeftMostPlanNodeOperator();
        context.setJdbcClient(jdbcClient);
        attachPlanBuildContext(phyiscalPlan.getRoot(), context);
        // Apply AS_A_WHOLE optimization rule to the physical plan
        for (Rule rule : context.getRules()) {
            if (rule.getRuleType() == RuleType.AS_A_WHOLE && rule.checkForRulePrecondition(phyiscalPlan.getRoot())) {
                // We create ruleContext each time we apply rule because rule may create a new plan and we want to keep all
                // the existing PlanStatistics.
                RuleContext ruleContext = RuleContext
                        .builder()
                        .setPlanStatistics(phyiscalPlan.getPlanStatistics())
                        .setJdbcClinet(context.getJdbcClient())
                        .setPlanNodeIdAllocator(context.getPlanNodeIdAllocator())
                        .setOperatorMap(context.getOperatorMap())
                        .setPlanBuildContext(context)
                        .build();
                phyiscalPlan = rule.applyToPhysicalPlan(phyiscalPlan.getRoot(), ruleContext);
                phyiscalPlan.getPlanStatistics().mergeRuleStatistics(rule.getRuleStatistics());
                context = phyiscalPlan.getRoot().getOperator().getPlanBuildContext();
            }
        }
        if (phyiscalPlan.getRoot().getOperator().getPlanBuildContext().getLeftMostPlanNodeOperator().getNoGoodListMap() == null &&
                ifUseTTJFamilyOperator(operatorMap, false)) {
            context.getLeftMostPlanNodeOperator().setNoGoodListMap(decideNoGoodListMapImpl(phyiscalPlan, context));
        }
        // FIXME: Below is a hack to ensure planStatististics NoGoodListMapClazz is not overriden by the rule application
        // The correct way is that rule should ensure planStatististics is properly preserved (e.g., createPhysicalPlanFromJoinOrdering
        // call in FindTheBestJoinOrderingJoinTreeWithDP doesn't preserve planStatististics, which should be fixed)
        if (phyiscalPlan.getRoot().getOperator().getPlanBuildContext().getLeftMostPlanNodeOperator().getNoGoodListMap() != null) {
            phyiscalPlan.getPlanStatistics().setNoGoodListMapClazz(phyiscalPlan.getRoot().getOperator().getPlanBuildContext().getLeftMostPlanNodeOperator().getNoGoodListMap().getClass().getCanonicalName());
        }
        return phyiscalPlan;
    }

    @Override
    public PlanNode visitPlan(PlanNode node, Pair<Map<OptType, List<Class<? extends Operator>>>, Integer> operatorMap)
    {
        List<PlanNode> children = node.getSources();
        for (PlanNode child : children) {
            if (child != null) {
                visitPlan(child, Pair.of(operatorMap.getKey(), operatorMap.getValue() + 1));
            }
        }
        return node.accept(this, operatorMap);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, Pair<Map<OptType, List<Class<? extends Operator>>>, Integer> operatorMap)
    {
        visitNode(node, operatorMap);
        // By default, we assume the left-deep plan. swap() in TupleBasedJoinOperator modifies this value because it changes the plan to
        // right-deep.
        node.getOperator().setOperatorAssociatedRelationSize(node.getRight().getOperator().getOperatorAssociatedRelationSize());
        TupleBasedJoinOperator joinOperator = (TupleBasedJoinOperator) node.getOperator();
        if (operatorMap.getLeft().get(OptType.join).contains(TupleBasedLeftSemiHashJoinOperator.class) ||
                operatorMap.getLeft().get(OptType.join).contains(TupleBasedLeftSemiBloomJoinOperator.class)) {
            joinOperator.construct(JoinType.LeftSemiJoin);
        }
        else if (operatorMap.getLeft().get(OptType.join).contains(TupleBasedLeftAntiHashJoinOperator.class)) {
            joinOperator.construct(JoinType.LeftAntiJoin);
        }
        else {
            joinOperator.construct(JoinType.NaturalJoin);
        }
        return node;
    }

    @Override
    public PlanNode visitTable(TableNode node, Pair<Map<OptType, List<Class<? extends Operator>>>, Integer> operatorMap)
    {
        node.setJdbcClient(jdbcClient);
        visitNode(node, operatorMap);
        return node;
    }

    @Override
    public PlanNode visitFullReducer(FullReducerNode node, Pair<Map<OptType, List<Class<? extends Operator>>>, Integer> operatorMap)
    {
        visitNode(node, operatorMap);
        return node;
    }

    private <T extends Operator> void visitNode(PlanNode node, Pair<Map<OptType, List<Class<? extends Operator>>>, Integer> operatorMap)
    {
        List<Class<? extends Operator>> operatorClazzList = operatorMap.getKey().get(node.getNodeType());
        requireNonNull(operatorClazzList,
                "operatorMap contains no operator implementation class for the given node type " + node.getNodeType());
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
            operator.setOperatorTraceDepth(operatorMap.getValue());
            node.setOperator(operator);
            // Apply IN_PLACE optimization rule to physical plan
            for (Rule rule : context.getRules()) {
                if (rule.getRuleType() == RuleType.IN_PLACE && rule.checkForRulePrecondition(node)) {
                    RuleContext ruleContext = RuleContext.builder()
                            .setOperator(operator)
                            .setPlanBuildContext(context)
                            .build();
                    rule.applyToPhysicalPlan(node, ruleContext);
                    // NOTE: maybe we have a risk of overriding field of RuleStatistics
                    // when merging but given current fields of RuleStatistics (mostly related to AS_A_WHOLE rule)
                    // we don't have this issue but should aware.
                    planStatistics.mergeRuleStatistics(rule.getRuleStatistics());
                }
            }
        }
        catch (NoSuchMethodException e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR,
                    String.format("There is no constructor found for %s", operatorClazz.getName()));
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
        }
    }

    public static <T extends Operator> T createTableScanOperator(TableNode tableNode,
                                                                 Constructor<? extends Operator> constructor,
                                                                 PlanBuildContext context,
                                                                 Map<Integer, List<Operator>> node2Operators)
    {
        T operator = null;
        try {
            if (tableNode.getMultiwayJoinNode() != null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace("tableNode.getMultiwayJoinNode: " + tableNode.getMultiwayJoinNode());
                }
                node2Operators.computeIfAbsent(tableNode.getMultiwayJoinNode().getNodeId(), k -> new ArrayList<>());
                // By default, i.e., non R_k node, we use normal table scan operator. The specified table scan operator
                // in operatorMap only impacts the table scan implementation of R_k.
                if (!tableNode.getMultiwayJoinNode().equals(context.getOrderedGraph().getRoot())) {
                    operator = (T) TupleBasedTableScanOperator.class.getConstructor().newInstance();
                }
                else {
                    operator = (T) constructor.newInstance();
                }
                operator.setMultiwayJoinNode(tableNode.getMultiwayJoinNode());
                node2Operators.get(tableNode.getMultiwayJoinNode().getNodeId()).add(operator);
            }
            else {
                operator = (T) constructor.newInstance();
            }
            operator.setSchemaTableName(tableNode.getSchemaTableName());
            setOperatorAssociatedRelationSize(operator, context.getJdbcClient());
            operator.setRecordTupleSourceProvider(decideRecordTupleSourceProviderImpl(operator.getSchemaTableName(), context));
            operator.setTableCatalog(context.getCatalogGroup().getTableCatalog(operator.getSchemaTableName()));
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
            if (ifUseTTJFamilyOperator(context.getOperatorMap(), true) &&
                    node.getRight().getOperator().getMultiwayJoinNode() != null) {
                operator.initializeContextObject();
            }
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

    private NoGoodListMap decideNoGoodListMapImpl(Plan physicalPlan, PlanBuildContext context)
    {
        PlanNode root = physicalPlan.getRoot();
        checkArgument(root.getOperator().getOperatorType() == OptType.join);
        Class<? extends Operator> operatorClass = root.getOperator().getClass();
        if (operatorClass == TupleBaseTreeTrackerOneBetaHashTableOperator.class) {
            physicalPlan.getPlanStatistics().setNoGoodListMapClazz(SimpleNoGoodListMap.class.getCanonicalName());
            return constructSimpleNoGoodListMap(context);
        }
        Pair<Boolean, SingleValueJavIntRowNoGoodListMap> pair = constructSingleValueJavIntRowNoGoodListMap(root, context);
        if (context.getLeftMostPlanNodeOperator().getRecordTupleSourceProviderClazz().equals(RecordIntTupleSourceProvider.class) && pair.getKey()) {
            physicalPlan.getPlanStatistics().setNoGoodListMapClazz(SingleValueJavIntRowNoGoodListMap.class.getCanonicalName());
            return pair.getRight();
        }
        Pair<Boolean, SingleValueJavNoGoodListMap> pair2 = constructSingleValueJavNoGoodListMap(root, context);
        if (pair2.getKey()) {
            physicalPlan.getPlanStatistics().setNoGoodListMapClazz(SingleValueJavNoGoodListMap.class.getCanonicalName());
            return pair2.getRight();
        }
        physicalPlan.getPlanStatistics().setNoGoodListMapClazz(DefaultNoGoodListMap.class.getCanonicalName());
        return constructDefaultNoGoodListMap(context);
    }

    private static RecordTupleSourceProvider decideRecordTupleSourceProviderImpl(SchemaTableName schemaTableName, PlanBuildContext context)
    {
        TableCatalog tableCatalog = context.getCatalogGroup().getTableCatalog(schemaTableName);
        if (tableCatalog.getTypeList().stream().allMatch(type -> type.equals(INTEGER))) {
            return new RecordIntTupleSourceProvider(new JdbcRecordSetProvider(context.getJdbcClient()));
        }
        return new RecordObjectTupleSourceProvider(new JdbcRecordSetProvider(context.getJdbcClient()));
    }

    private static boolean ifUseTTJFamilyOperator(Map<OptType, List<Class<? extends Operator>>> operatorMap, boolean ignoreTableScan)
    {
        if (!ignoreTableScan) {
            return (operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class) &&
                    operatorMap.get(OptType.table).contains(TupleBasedHighPerfTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TreeTrackerJoinOperator.class) &&
                            operatorMap.get(OptType.table).contains(TreeTrackerTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TreeTrackerJoinV2Operator.class) &&
                            operatorMap.get(OptType.table).contains(TreeTrackerTableScanV2Operator.class)) ||
                    (operatorMap.get(OptType.join).contains(TupleBaseTreeTrackerOneBetaHashTableOperator.class) ||
                            operatorMap.get(OptType.table).contains(TupleBaseTreeTrackerOneBetaTableScanOperator.class)) ||
                    (operatorMap.get(OptType.join).contains(TreeTrackerBFJoinOperator.class) &&
                            operatorMap.get(OptType.table).contains(TupleBasedHighPerfTableScanOperator.class));
        }
        else {
            return operatorMap.get(OptType.join).contains(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class) ||
                    operatorMap.get(OptType.join).contains(TreeTrackerJoinOperator.class) ||
                    operatorMap.get(OptType.join).contains(TreeTrackerJoinV2Operator.class) ||
                    operatorMap.get(OptType.join).contains(TupleBaseTreeTrackerOneBetaHashTableOperator.class) ||
                    operatorMap.get(OptType.join).contains(TreeTrackerBFJoinOperator.class);
        }
    }
}
