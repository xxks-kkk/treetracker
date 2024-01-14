package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.TestFullReducerOperator.compareSemiJoinOrdering;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoinGraphHeuristicsFromPT
{
    private Logger traceLogger = LogManager.getLogger(TestJoinGraphHeuristicsFromPT.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestJoinGraphHeuristicsFromPT";
    private StatisticsInformationPrinter printer;
    private TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TestJoinGraphHeuristicsFromPT.class.getName(), Level.TRACE);
            Configurator.setAllLevels(AttachFullReducer.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TestFullReducerOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(RandomPhysicalPlanBuilder.class.getName(), Level.TRACE);
        }

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class), Optional.of(TupleBasedHashJoinOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    public static class TestJoinGraphHeuristicsFromPTTestGraphs
            implements TestCases
    {
        private final String schemaName;
        private final JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestJoinGraphHeuristicsFromPTTestGraphs(TestingPhysicalPlanBase base)
        {
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        /**
         * This is the example from PT paper on TPC-H Q5
         */
        public Triple<MultiwayJoinOrderedGraph, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>> graph()
        {
            String schemaName = base.getDatabase().getSchemaName();
            JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
            String region = this.getClass().getSimpleName() + "_region";
            SchemaTableName schemaTableNameRegion = new SchemaTableName(schemaName, region);
            List<String> regionAttr = List.of("regionkey");
            if (jdbcClient.getTableHandle(schemaTableNameRegion) == null) {
                List<List<RelationalValue>> relationValRegion = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(
                        schemaName,
                        region,
                        regionAttr,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValRegion);
            }
            MultiwayJoinNode nodeRegion = new MultiwayJoinNode(schemaTableNameRegion, regionAttr, new MultiwayJoinDomain());

            String nation = this.getClass().getSimpleName() + "_nation";
            SchemaTableName schemaTableNameNation = new SchemaTableName(schemaName, nation);
            List<String> nationAttr = List.of("nationkey", "regionkey");
            if (jdbcClient.getTableHandle(schemaTableNameNation) == null) {
                List<List<RelationalValue>> relationValNation = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(2), IntegerValue.of(3))));
                jdbcClient.ingestRelation(
                        schemaName,
                        nation,
                        nationAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValNation);
            }
            MultiwayJoinNode nodeNation = new MultiwayJoinNode(schemaTableNameNation, nationAttr, new MultiwayJoinDomain());

            String supplier = this.getClass().getSimpleName() + "_supplier";
            SchemaTableName schemaTableNameSupplier = new SchemaTableName(schemaName, supplier);
            List<String> suppAttr = List.of("nationkey", "suppkey");
            if (jdbcClient.getTableHandle(schemaTableNameSupplier) == null) {
                List<List<RelationalValue>> relationValSupplier = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(2)),
                        List.of(IntegerValue.of(3), IntegerValue.of(4)),
                        List.of(IntegerValue.of(5), IntegerValue.of(6))));
                jdbcClient.ingestRelation(
                        schemaName,
                        supplier,
                        suppAttr,
                        new ArrayList<>(List.of(INTEGER, INTEGER)),
                        relationValSupplier);
            }
            MultiwayJoinNode nodeSupplier = new MultiwayJoinNode(schemaTableNameSupplier, suppAttr, new MultiwayJoinDomain());

            String customer = this.getClass().getSimpleName() + "_customer";
            SchemaTableName schemaTableNameCustomer = new SchemaTableName(schemaName, customer);
            List<String> customerAttr = List.of("nationkey", "custkey");
            if (jdbcClient.getTableHandle(schemaTableNameCustomer) == null) {
                List<List<RelationalValue>> relationValCustomer = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(3), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(5), IntegerValue.of(6)),
                        Arrays.asList(IntegerValue.of(7), IntegerValue.of(8))));
                jdbcClient.ingestRelation(
                        schemaName,
                        customer,
                        customerAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValCustomer);
            }
            MultiwayJoinNode nodeCustomer = new MultiwayJoinNode(schemaTableNameCustomer, customerAttr, new MultiwayJoinDomain());

            String orders = this.getClass().getSimpleName() + "_orders";
            SchemaTableName schemaTableNameOrders = new SchemaTableName(schemaName, orders);
            List<String> ordersAttr = List.of("orderkey", "custkey");
            if (jdbcClient.getTableHandle(schemaTableNameOrders) == null) {
                List<List<RelationalValue>> relationValOrders = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(3), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(5), IntegerValue.of(6)),
                        Arrays.asList(IntegerValue.of(7), IntegerValue.of(8)),
                        Arrays.asList(IntegerValue.of(9), IntegerValue.of(10))));
                jdbcClient.ingestRelation(
                        schemaName,
                        orders,
                        ordersAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValOrders);
            }
            MultiwayJoinNode nodeOrders = new MultiwayJoinNode(schemaTableNameOrders, ordersAttr, new MultiwayJoinDomain());

            String lineItem = this.getClass().getSimpleName() + "_lineitem";
            SchemaTableName schemaTableNameLineItem = new SchemaTableName(schemaName, lineItem);
            List<String> lineItemAttr = List.of("suppkey", "orderkey");
            if (jdbcClient.getTableHandle(schemaTableNameLineItem) == null) {
                List<List<RelationalValue>> relationValLineItem = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(3), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(5), IntegerValue.of(6)),
                        Arrays.asList(IntegerValue.of(7), IntegerValue.of(8)),
                        Arrays.asList(IntegerValue.of(9), IntegerValue.of(10)),
                        Arrays.asList(IntegerValue.of(11), IntegerValue.of(12))));
                jdbcClient.ingestRelation(
                        schemaName,
                        lineItem,
                        lineItemAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValLineItem);
            }
            MultiwayJoinNode nodeLineItem = new MultiwayJoinNode(schemaTableNameLineItem, lineItemAttr, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeRegion, nodeNation),
                    asEdge(nodeNation, nodeCustomer),
                    asEdge(nodeNation, nodeSupplier),
                    asEdge(nodeSupplier, nodeCustomer),
                    asEdge(nodeCustomer, nodeOrders),
                    asEdge(nodeSupplier, nodeLineItem),
                    asEdge(nodeOrders, nodeLineItem))), nodeLineItem);
            List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedSemiJoinOrder = List.of(
                    Pair.of(nodeNation, nodeRegion),
                    Pair.of(nodeSupplier, nodeNation),
                    Pair.of(nodeCustomer, nodeNation),
                    Pair.of(nodeCustomer, nodeSupplier),
                    Pair.of(nodeOrders, nodeCustomer),
                    Pair.of(nodeLineItem, nodeOrders),
                    Pair.of(nodeLineItem, nodeSupplier));
            List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedTopDownSemiJoinOrder = List.of(
                    Pair.of(nodeOrders, nodeLineItem),
                    Pair.of(nodeCustomer, nodeOrders),
                    Pair.of(nodeSupplier, nodeLineItem),
                    Pair.of(nodeSupplier, nodeCustomer),
                    Pair.of(nodeNation, nodeSupplier),
                    Pair.of(nodeNation, nodeCustomer),
                    Pair.of(nodeRegion, nodeNation)
            );
            return Triple.of(orderedGraph, expectedSemiJoinOrder, expectedTopDownSemiJoinOrder);
        }

        /**
         * This is SSB Q2.1. The predicate passing order should be identical to LIP and Yannakakis because ssb.lineorder
         * has the largest size (6001173) and join tree is identical to query graph
         * SELECT count(*)
         * FROM ssb.lineorder, ssb.date, ssb.part, ssb.supplier
         * WHERE lineorder.datekey = date.datekey
         * AND lineorder.partkey = part.partkey
         * AND lineorder.suppkey = supplier.suppkey
         * AND p_category = 'MFGR#12'
         * AND s_region = 'AMERICA';
         */
        public Triple<MultiwayJoinOrderedGraph, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>> graph2()
        {
            String schemaName = base.getDatabase().getSchemaName();
            JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
            String date = this.getClass().getSimpleName() + "_date";
            SchemaTableName schemaTableNameDate = new SchemaTableName(schemaName, date);
            List<String> dateAttr = List.of("datekey");
            if (jdbcClient.getTableHandle(schemaTableNameDate) == null) {
                List<List<RelationalValue>> relationValDate = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(
                        schemaName,
                        date,
                        dateAttr,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValDate);
            }
            MultiwayJoinNode nodeDate = new MultiwayJoinNode(schemaTableNameDate, dateAttr, new MultiwayJoinDomain());

            String part = this.getClass().getSimpleName() + "_part";
            SchemaTableName schemaTableNamePart = new SchemaTableName(schemaName, part);
            List<String> partAttr = List.of("partkey");
            if (jdbcClient.getTableHandle(schemaTableNamePart) == null) {
                List<List<RelationalValue>> relationValPart = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1)),
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(
                        schemaName,
                        part,
                        partAttr,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValPart);
            }
            MultiwayJoinNode nodePart = new MultiwayJoinNode(schemaTableNamePart, partAttr, new MultiwayJoinDomain());

            String supplier = this.getClass().getSimpleName() + "_suppliers";
            SchemaTableName schemaTableNameSupplier = new SchemaTableName(schemaName, supplier);
            List<String> supplierAttr = List.of("suppkey");
            if (jdbcClient.getTableHandle(schemaTableNameSupplier) == null) {
                List<List<RelationalValue>> relationValSupplier = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1)),
                        List.of(IntegerValue.of(3)),
                        List.of(IntegerValue.of(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        supplier,
                        supplierAttr,
                        new ArrayList<>(List.of(INTEGER)),
                        relationValSupplier);
            }
            MultiwayJoinNode nodeSupplier = new MultiwayJoinNode(schemaTableNameSupplier, supplierAttr, new MultiwayJoinDomain());

            String lineOrder = this.getClass().getSimpleName() + "_lineorder";
            SchemaTableName schemaTableNameLineOrder = new SchemaTableName(schemaName, lineOrder);
            List<String> lineOrderAttr = List.of("datekey", "partkey", "suppkey");
            if (jdbcClient.getTableHandle(schemaTableNameLineOrder) == null) {
                List<List<RelationalValue>> relationValLineOrder = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(3)),
                        Arrays.asList(IntegerValue.of(3), IntegerValue.of(4), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(5), IntegerValue.of(6), IntegerValue.of(5)),
                        Arrays.asList(IntegerValue.of(7), IntegerValue.of(8), IntegerValue.of(6)),
                        Arrays.asList(IntegerValue.of(9), IntegerValue.of(10), IntegerValue.of(7)),
                        Arrays.asList(IntegerValue.of(11), IntegerValue.of(12), IntegerValue.of(8))));
                jdbcClient.ingestRelation(
                        schemaName,
                        lineOrder,
                        lineOrderAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER, INTEGER)),
                        relationValLineOrder);
            }
            MultiwayJoinNode nodeLineOrder = new MultiwayJoinNode(schemaTableNameLineOrder, lineOrderAttr, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeLineOrder, nodeDate),
                    asEdge(nodeLineOrder, nodePart),
                    asEdge(nodeLineOrder, nodeSupplier))), nodeLineOrder);
            List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedSemiJoinOrder = List.of(
                    Pair.of(nodeLineOrder, nodePart),
                    Pair.of(nodeLineOrder, nodeSupplier),
                    Pair.of(nodeLineOrder, nodeDate));
            List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedTopDownSemiJoinOrder = List.of(
                    Pair.of(nodePart, nodeLineOrder),
                    Pair.of(nodeSupplier, nodeLineOrder),
                    Pair.of(nodeDate, nodeLineOrder)
            );
            return Triple.of(orderedGraph, expectedSemiJoinOrder, expectedTopDownSemiJoinOrder);
        }

        /**
         * TPC-H Q18W
         * tpch.q18w_orders(orderkey,custkey)
         * |tpch_int.customer(custkey,nationkey)
         * |tpch_int.lineitem(orderkey,partkey,suppkey)
         */
        public Triple<MultiwayJoinOrderedGraph, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>> graph3()
        {
            String schemaName = base.getDatabase().getSchemaName();
            JdbcClient jdbcClient = base.getDatabase().getJdbcClient();

            String customer = this.getClass().getSimpleName() + "_customer2";
            SchemaTableName schemaTableNameCustomer = new SchemaTableName(schemaName, customer);
            List<String> customerAttr = List.of("nationkey", "custkey");
            if (jdbcClient.getTableHandle(schemaTableNameCustomer) == null) {
                List<List<RelationalValue>> relationValCustomer = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(3), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(5), IntegerValue.of(6)),
                        Arrays.asList(IntegerValue.of(7), IntegerValue.of(8))));
                jdbcClient.ingestRelation(
                        schemaName,
                        customer,
                        customerAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValCustomer);
            }
            MultiwayJoinNode nodeCustomer = new MultiwayJoinNode(schemaTableNameCustomer, customerAttr, new MultiwayJoinDomain());

            String orders = this.getClass().getSimpleName() + "_orders2";
            SchemaTableName schemaTableNameOrders = new SchemaTableName(schemaName, orders);
            List<String> ordersAttr = List.of("orderkey", "custkey");
            if (jdbcClient.getTableHandle(schemaTableNameOrders) == null) {
                List<List<RelationalValue>> relationValOrders = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(
                        schemaName,
                        orders,
                        ordersAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValOrders);
            }
            MultiwayJoinNode nodeOrders = new MultiwayJoinNode(schemaTableNameOrders, ordersAttr, new MultiwayJoinDomain());

            String lineItem = this.getClass().getSimpleName() + "_lineitem2";
            SchemaTableName schemaTableNameLineItem = new SchemaTableName(schemaName, lineItem);
            List<String> lineItemAttr = List.of("suppkey", "orderkey");
            if (jdbcClient.getTableHandle(schemaTableNameLineItem) == null) {
                List<List<RelationalValue>> relationValLineItem = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(3), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(5), IntegerValue.of(6)),
                        Arrays.asList(IntegerValue.of(7), IntegerValue.of(8)),
                        Arrays.asList(IntegerValue.of(9), IntegerValue.of(10)),
                        Arrays.asList(IntegerValue.of(11), IntegerValue.of(12))));
                jdbcClient.ingestRelation(
                        schemaName,
                        lineItem,
                        lineItemAttr,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValLineItem);
            }
            MultiwayJoinNode nodeLineItem = new MultiwayJoinNode(schemaTableNameLineItem, lineItemAttr, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeOrders, nodeLineItem),
                    asEdge(nodeOrders, nodeCustomer))), nodeOrders);
            List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedSemiJoinOrder = List.of(
                    Pair.of(nodeCustomer, nodeOrders),
                    Pair.of(nodeLineItem, nodeOrders));
            List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedTopDownSemiJoinOrder = List.of(
                    Pair.of(nodeOrders, nodeLineItem),
                    Pair.of(nodeOrders, nodeCustomer));
            return Triple.of(orderedGraph, expectedSemiJoinOrder, expectedTopDownSemiJoinOrder);
        }
    }

    private Object[][] testJoinGraphHeuristicsFromPTDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestJoinGraphHeuristicsFromPTTestGraphs.class)));
    }

    @ParameterizedTest
    @MethodSource("testJoinGraphHeuristicsFromPTDataProvider")
    public void testJoinGraphHeuristicsFromPT(Triple<MultiwayJoinOrderedGraph, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>, List<Pair<MultiwayJoinNode, MultiwayJoinNode>>> triple)
    {
        PlanBuildContext context = builder()
                .setRules(List.of(new AttachFullReducer()))
                .setPlanNodeIdAllocator(base.getIdAllocator())
                .setOperatorMap(base.getOperatorMap())
                .setOrderedGraph(triple.getLeft())
                .enableJoinGraphHeuristicFromPT(true)
                .build();
        Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.empty());
        base.testPhysicalPlanExecution(planPair);
        Operator rootOperator = planPair.getLeft().getRoot().getOperator();
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.Yannakakis)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        YannakakisAggregateStatisticsInformation aggStatsInfo = (YannakakisAggregateStatisticsInformation) factory.get();
        compareSemiJoinOrdering(triple.getMiddle(), aggStatsInfo.getBottomUpSemiJoinOrdering());
        compareSemiJoinOrdering(triple.getRight(), aggStatsInfo.getTopDownSemiJoinOrdering());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
