package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
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
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
public class TestYannakakisAggregateStatisticsInformation
{
    private Logger traceLogger = LogManager.getLogger(TestYannakakisAggregateStatisticsInformation.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestYannakakisAggregateStatisticsInformation";

    @BeforeAll
    public void setUp()
    {
        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(AggregateStatisticsInformation.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(YannakakisAggregateStatisticsInformation.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.DEBUG);
        }

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new AttachFullReducer()),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class))));
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
        YannakakisAggregateStatisticsInformation yannakakisAggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, yannakakisAggregateStatisticsInformation.getNumberOfR1Assignments());
    }

    public static class TestNumberOfR1AssignmentCases
            implements TestCases
    {
        private TestingPhysicalPlanBase base;

        public TestNumberOfR1AssignmentCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfR1Assignment()
        {
            TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases cases = new TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases(base);
            Triple<Plan, List<Operator>, Integer> testCase = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(testCase.getLeft(), testCase.getMiddle(), 63);
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfR1Assignment2()
        {
            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour();
            // See cost-model4.pdf
            return Triple.of(pair.getLeft(), pair.getRight(), 50);
        }
    }

    public static class TestNumberOfHashTableProbeCases
            implements TestCases
    {
        private TestingPhysicalPlanBase base;

        public TestNumberOfHashTableProbeCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfHashTableProbe()
        {
            TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases cases = new TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases(base);
            Triple<Plan, List<Operator>, Integer> testCase = cases.physicalPlanForTestEstimateCostOfTTJ();
            // the total number of hash table probe within full reducer + total number of hash table within the join plan
            return Triple.of(testCase.getLeft(), testCase.getMiddle(), 21);
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfHashTableProbe2()
        {
            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour();
            // the total number of hash table probe within full reducer + total number of hash table with the join plan
            return Triple.of(pair.getLeft(), pair.getRight(), 13);
        }
    }

    private Object[][] testNumberOfHashTableProbeDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestNumberOfHashTableProbeCases.class)));
    }

    private YannakakisAggregateStatisticsInformation testStatistics(Triple<Plan, List<Operator>, Integer> triple)
    {
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
        Operator rootOperator = triple.getLeft().getRoot().getOperator();
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.Yannakakis)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        return (YannakakisAggregateStatisticsInformation) factory.get();
    }

    @ParameterizedTest
    @MethodSource("testNumberOfHashTableProbeDataProvider")
    public void testNumberOfHashTableProbe(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("expectedValue:\n" + expectedValue);
        }
        YannakakisAggregateStatisticsInformation yannakakisAggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, yannakakisAggregateStatisticsInformation.getNumberOfHashTableProbe());
    }

    public static class TestNumberOfHashTableProbeWithFullReducerCases
            implements TestCases
    {
        private TestingPhysicalPlanBase base;

        public TestNumberOfHashTableProbeWithFullReducerCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfHashTableProbe()
        {
            TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases cases = new TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases(base);
            Triple<Plan, List<Operator>, Integer> testCase = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(testCase.getLeft(), testCase.getMiddle(), 18);
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfHashTableProbe2()
        {
            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour();
            return Triple.of(pair.getLeft(), pair.getRight(), 10);
        }
    }

    private Object[][] testNumberOfHashTableProbeWithinFullReducerDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestNumberOfHashTableProbeWithFullReducerCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testNumberOfHashTableProbeWithinFullReducerDataProvider")
    public void testNumberOfHashTableProbeWithinFullReducer(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("expectedValue:\n" + expectedValue);
        }
        YannakakisAggregateStatisticsInformation yannakakisAggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, yannakakisAggregateStatisticsInformation.getNumberOfHashTableProbeWithinFullReducer());
    }

    public static class TestNumberOfHashTableBuildTuplesWithinFullReducerCases
            implements TestCases
    {
        private TestingPhysicalPlanBase base;

        public TestNumberOfHashTableBuildTuplesWithinFullReducerCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfHashTableBuildTuplesWithinFullReducer()
        {
            TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases cases = new TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases(base);
            Triple<Plan, List<Operator>, Integer> testCase = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(testCase.getLeft(), testCase.getMiddle(), 7);
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestNumberOfHashTableBuildTuplesWithinFullReducer2()
        {
            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour();
            return Triple.of(pair.getLeft(), pair.getRight(), 6);
        }
    }

    private Object[][] testNumberOfHashTableBuildTuplesWithinFullReducerDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestNumberOfHashTableBuildTuplesWithinFullReducerCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testNumberOfHashTableBuildTuplesWithinFullReducerDataProvider")
    public void testNumberOfHashTableBuildTuplesWithinFullReducer(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("expectedValue:\n" + expectedValue);
        }
        YannakakisAggregateStatisticsInformation yannakakisAggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, yannakakisAggregateStatisticsInformation.getNumberOfHashTableBuildTuplesWithinFullReducer());
    }

    public static class TestTotalDomainSizeCases
            implements TestCases
    {
        private TestingPhysicalPlanBase base;

        public TestTotalDomainSizeCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestTotalDomainSize()
        {
            TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases cases = new TestTTJAggregateStatisticsInformation.TestEstimateCostOfTTJCases(base);
            Triple<Plan, List<Operator>, Integer> testCase = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(testCase.getLeft(), testCase.getMiddle(), 4);
        }

        public Triple<Plan, List<Operator>, Integer> physicalPlanForTestTotalDomainSize2()
        {
            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour();
            return Triple.of(pair.getLeft(), pair.getRight(), 3);
        }
    }

    private Object[][] testTotalDomainSizeDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestTotalDomainSizeCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testTotalDomainSizeDataProvider")
    public void testTotalDomainSize(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("expectedValue:\n" + expectedValue);
        }
        YannakakisAggregateStatisticsInformation yannakakisAggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, yannakakisAggregateStatisticsInformation.getInnerDomainSize());
    }

    @Test
    public void testEarlyStop()
    {
        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String relationPrefix = this.getClass().getSimpleName();

        String relationT = relationPrefix + "_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        List<String> attributesT = new ArrayList<>(List.of("a", "b"));
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(List.of(IntegerValue.of(2), IntegerValue.of(3))));
            jdbcClient.ingestRelation(schemaName, relationT, attributesT,
                    new ArrayList<>(List.of(INTEGER, INTEGER)), relationValT);
        }
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, attributesT, new MultiwayJoinDomain());

        String relationS = relationPrefix + "_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        List<String> attributeS = new ArrayList<>(Arrays.asList("b", "c"));
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(List.of(IntegerValue.of(3), IntegerValue.of(3)),
                    Arrays.asList(IntegerValue.of(2), IntegerValue.of(1))));
            jdbcClient.ingestRelation(schemaName, relationS, attributeS, new ArrayList<>(Arrays.asList(INTEGER, INTEGER)), relationValS);
        }
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, attributeS, new MultiwayJoinDomain());

        String relationR = relationPrefix + "_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        List<String> attributeR = new ArrayList<>(Arrays.asList("c", "d"));
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
            jdbcClient.ingestRelation(schemaName, relationR, attributeR, new ArrayList<>(Arrays.asList(INTEGER, INTEGER)), relationValR);
        }
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributeR, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeR))), nodeT);

        SemiJoinOrdering semiJoinOrdering = new SemiJoinOrdering(
                List.of(Pair.of(nodeS, nodeR), Pair.of(nodeT, nodeS)),
                orderedGraph);

        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanForYannakakis(semiJoinOrdering);
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getLeft().getRoot().getOperator();
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.Yannakakis)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        YannakakisAggregateStatisticsInformation aggStatsInfo = (YannakakisAggregateStatisticsInformation) factory.get();
        assertTrue(aggStatsInfo.isEarlyStoppedDueToBottomUpPass());
        assertFalse(aggStatsInfo.isEarlyStoppedDueToTopDownPass());
        Pair<MultiwayJoinNode, MultiwayJoinNode> semijoinCausingEarlyStop = aggStatsInfo.getSemijoinCausingEarlyStop();
        assertEquals(schemaTableNameT, semijoinCausingEarlyStop.getLeft().getSchemaTableName());
        assertEquals(schemaTableNameS, semijoinCausingEarlyStop.getRight().getSchemaTableName());
    }
}
