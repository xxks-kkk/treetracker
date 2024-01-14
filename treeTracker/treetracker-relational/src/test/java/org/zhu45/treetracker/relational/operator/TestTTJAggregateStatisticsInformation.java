package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.cost.TestJoinTreeCostEstProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.TTJAggregateStatisticsInformation.generateDSR;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTTJAggregateStatisticsInformation
{
    private Logger traceLogger = LogManager.getLogger(TestTTJAggregateStatisticsInformation.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestTTJAggregateStatisticsInformation";
    private StatisticsInformationPrinter printer;

    @Test
    public void testGenerateDSR()
    {
        String schema = "testGenerateDSR";
        MultiwayJoinNode c = new MultiwayJoinNode(new SchemaTableName(schema, "c"), List.of("y"), null);
        MultiwayJoinNode r = new MultiwayJoinNode(new SchemaTableName(schema, "r"), List.of("x", "y"), null);
        MultiwayJoinNode b = new MultiwayJoinNode(new SchemaTableName(schema, "b"), List.of("x", "y"), null);

        TTJAggregateStatisticsInformation.CostEstimationContext context = new TTJAggregateStatisticsInformation.CostEstimationContext();
        context.multiwayJoinOrderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(c, r),
                asEdge(r, b))), c);
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr = generateDSR(context.multiwayJoinOrderedGraph);
        assertEquals(2, dsr.get(c).keySet().size());
        assertEquals(0, dsr.get(c).get(c));
        assertEquals(1, dsr.get(c).get(r));
        assertEquals(2, dsr.get(r).keySet().size());
        assertEquals(0, dsr.get(r).get(r));
        assertEquals(1, dsr.get(r).get(b));
        assertEquals(1, dsr.get(b).keySet().size());
        assertEquals(0, dsr.get(b).get(b));
    }

    @Test
    public void testGenerateDSR2()
    {
        String schema = "testGenerateDSR";
        MultiwayJoinNode lineItem = new MultiwayJoinNode(new SchemaTableName(schema, "lineItem"),
                List.of("orderKey", "partKey", "suppKey"), null);
        MultiwayJoinNode orders = new MultiwayJoinNode(new SchemaTableName(schema, "orders"),
                List.of("orderKey", "custKey"), null);
        MultiwayJoinNode customer = new MultiwayJoinNode(new SchemaTableName(schema, "customer"),
                List.of("custKey", "nationKey"), null);

        TTJAggregateStatisticsInformation.CostEstimationContext context = new TTJAggregateStatisticsInformation.CostEstimationContext();
        context.multiwayJoinOrderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(lineItem, orders),
                asEdge(orders, customer)
        )), lineItem);

        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr = generateDSR(context.multiwayJoinOrderedGraph);
        assertEquals(2, dsr.get(lineItem).keySet().size());
        assertEquals(0, dsr.get(lineItem).get(lineItem));
        assertEquals(1, dsr.get(lineItem).get(orders));
        assertEquals(2, dsr.get(orders).keySet().size());
        assertEquals(0, dsr.get(orders).get(orders));
        assertEquals(1, dsr.get(orders).get(customer));
        assertEquals(1, dsr.get(customer).keySet().size());
        assertEquals(0, dsr.get(customer).get(customer));
    }

    @Test
    public void testGenerateDSR3()
    {
        String schema = "testGenerateDSR";
        MultiwayJoinNode mc = new MultiwayJoinNode(new SchemaTableName(schema, "mc"),
                List.of("movie_id", "company_id", "company_type_id"), null);
        MultiwayJoinNode ci = new MultiwayJoinNode(new SchemaTableName(schema, "ci"),
                List.of("person_id", "movie_id", "person_role_id", "role_id"), null);
        MultiwayJoinNode n = new MultiwayJoinNode(new SchemaTableName(schema, "n"),
                List.of("person_id"), null);
        MultiwayJoinNode mk = new MultiwayJoinNode(new SchemaTableName(schema, "mk"),
                List.of("movie_id", "keyword_id"), null);
        MultiwayJoinNode k = new MultiwayJoinNode(new SchemaTableName(schema, "k"),
                List.of("keyword_id"), null);
        MultiwayJoinNode cn = new MultiwayJoinNode(new SchemaTableName(schema, "cn"),
                List.of("company_id"), null);
        MultiwayJoinNode mi = new MultiwayJoinNode(new SchemaTableName(schema, "mi"),
                List.of("movie_id", "info_type_id"), null);
        MultiwayJoinNode mii = new MultiwayJoinNode(new SchemaTableName(schema, "mii"),
                List.of("movie_id", "info_type_id"), null);
        MultiwayJoinNode it = new MultiwayJoinNode(new SchemaTableName(schema, "it"),
                List.of("info_type_id"), null);
        MultiwayJoinNode t = new MultiwayJoinNode(new SchemaTableName(schema, "t"),
                List.of("movie_id", "kind_id"), null);

        TTJAggregateStatisticsInformation.CostEstimationContext context = new TTJAggregateStatisticsInformation.CostEstimationContext();
        context.multiwayJoinOrderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(mc, ci),
                asEdge(ci, n),
                asEdge(mc, mk),
                asEdge(mk, k),
                asEdge(mc, cn),
                asEdge(mc, mi),
                asEdge(mi, mii),
                asEdge(mii, it),
                asEdge(mc, t))), mc);

        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr = generateDSR(context.multiwayJoinOrderedGraph);
        assertEquals(6, dsr.get(mc).size());
        assertEquals(0, dsr.get(mc).get(mc));
        assertEquals(1, dsr.get(mc).get(ci));
        assertEquals(3, dsr.get(mc).get(mk));
        assertEquals(5, dsr.get(mc).get(cn));
        assertEquals(6, dsr.get(mc).get(mi));
        assertEquals(9, dsr.get(mc).get(t));
    }

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(DatabaseSuppler.class.getName(), Level.INFO);
            Configurator.setAllLevels(TestingMultiwayJoinDatabaseComplex.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(ExecutionNormal.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TTJAggregateStatisticsInformation.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(AggregateStatisticsInformation.class.getName(), Level.DEBUG);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    private Object[][] testEstimateCostOfTTJDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestEstimateCostOfTTJCases.class)));
    }

    private TTJAggregateStatisticsInformation testStatistics(Triple<Plan, List<Operator>, Integer> triple)
    {
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
        Operator rootOperator = triple.getLeft().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("statistics:\n" + statistics);
        }
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.TTJHP)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        return (TTJAggregateStatisticsInformation) factory.get();
    }

    @ParameterizedTest
    @MethodSource("testEstimateCostOfTTJDataProvider")
    public void testEstimateCostOfTTJ(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("expectedValue:\n" + expectedValue);
        }
        TTJAggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, aggregateStatisticsInformation.getEstimateCostOfTTJ());
    }

    public static class TestEstimateCostOfTTJCases
            implements TestCases
    {
        private String schemaName;
        private JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestEstimateCostOfTTJCases(TestingPhysicalPlanBase base)
        {
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestEstimateCostOfTTJ()
        {
            String relationC = this.getClass().getSimpleName() + "_C";
            SchemaTableName schemaTableNameC = new SchemaTableName(schemaName, relationC);
            List<String> attributesC = List.of("y");
            if (jdbcClient.getTableHandle(schemaTableNameC) == null) {
                List<List<RelationalValue>> relationValC = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(6)),
                        Arrays.asList(new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationC,
                        attributesC,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValC);
            }
            MultiwayJoinNode nodeC = new MultiwayJoinNode(schemaTableNameC, attributesC, new MultiwayJoinDomain());

            String relationR = this.getClass().getSimpleName() + "_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            List<String> attributesR = List.of("x", "y");
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(2), new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(6)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR,
                        attributesR,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValR);
            }
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributesR, new MultiwayJoinDomain());

            String relationB = this.getClass().getSimpleName() + "_B";
            SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
            List<String> attributesB = List.of("x", "y");
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(2), new IntegerValue(6)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationB,
                        attributesB,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValB);
            }
            MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributesB, new MultiwayJoinDomain());

            String relationG = this.getClass().getSimpleName() + "_G";
            SchemaTableName schemaTableNameG = new SchemaTableName(schemaName, relationG);
            List<String> attributesG = List.of("y");
            if (jdbcClient.getTableHandle(schemaTableNameG) == null) {
                List<List<RelationalValue>> relationValG = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(5)),
                        Arrays.asList(new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationG,
                        attributesG,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValG);
            }
            MultiwayJoinNode nodeG = new MultiwayJoinNode(schemaTableNameG, attributesG, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeC, nodeR),
                    asEdge(nodeR, nodeB),
                    asEdge(nodeB, nodeG))), nodeC);

            Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                    Optional.empty(),
                    Optional.empty());
            return Triple.of(pair.getLeft(), pair.getRight(), 23);
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlan2ForTestEstimateCostOfTTJ()
        {
            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour();
            return Triple.of(pair.getLeft(), pair.getRight(), 8);
        }
    }

    private Object[][] testNumberOfR1AssignmentDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestNumberOfR1AssignmentCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testNumberOfR1AssignmentDataProvider")
    public void testNumberOfR1Assignment(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("expectedValue:\n" + expectedValue);
        }
        TTJAggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, aggregateStatisticsInformation.getNumberOfR1Assignments());
    }

    public static class TestNumberOfR1AssignmentCases
            implements TestCases
    {
        private String schemaName;
        private JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestNumberOfR1AssignmentCases(TestingPhysicalPlanBase base)
        {
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfR1Assignment()
        {
            TestEstimateCostOfTTJCases cases = new TestEstimateCostOfTTJCases(base);
            Triple<Plan, List<Operator>, Integer> testCase = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(testCase.getLeft(), testCase.getMiddle(), 26);
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfR1Assignment2()
        {
            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour();
            return Triple.of(pair.getLeft(), pair.getRight(), 16);
        }
    }

    private Object[][] testNumberOfNoGoodTuplesDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestJoinTreeCostEstProvider.TestCostModelCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testNumberOfNoGoodTuplesDataProvider")
    public void testNumberOfNoGoodTuples(Triple<Plan, List<Operator>, Integer> triple)
    {
        TTJAggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(triple);
        assertEquals((long) triple.getRight(), aggregateStatisticsInformation.getNumberOfDanglingTuples());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
