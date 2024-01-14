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
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.cost.TestJoinTreeCostEstProvider;

import java.util.ArrayList;
import java.util.Arrays;
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
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTTJAggregateStatisticsInformationWithTTJV2Operator
{
    private Logger traceLogger = LogManager.getLogger(TestTTJAggregateStatisticsInformationWithTTJV2Operator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestTTJAggregateStatisticsInformationWithTTJV2Operator";
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TreeTrackerTableScanV2Operator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TreeTrackerJoinV2Operator.class.getName(), Level.TRACE);
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
                Optional.of(createMap(Optional.of(TreeTrackerTableScanV2Operator.class),
                        Optional.of(TreeTrackerJoinV2Operator.class))));
        printer = new StatisticsInformationPrinter();
    }

    private Object[][] testEstimateCostOfTTJDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases.class)));
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
                .setJoinOperator(JoinOperator.TTJV2)
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
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestTTJAggregateStatisticsInformation.TestNumberOfR1AssignmentCases.class)));
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
