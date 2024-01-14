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
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFullReducerVanillaOperator
{
    private Logger traceLogger = LogManager.getLogger(TestFullReducerOperator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestFullReducerVanillaOperator";
    private StatisticsInformationPrinter printer;
    private TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(FullReducerOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedLeftSemiBloomJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TestFullReducerOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TestFullReducerVanillaOperator.class.getName(), Level.DEBUG);
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

    public static class TestFullReducerTestCases
            implements TestCases
    {
        private TestingPhysicalPlanBase base;
        private PlanBuildContext.Builder builder;
        private TestFullReducerOperator.TestFullReducerTestGraphs graphs;

        public TestFullReducerTestCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
            this.graphs = new TestFullReducerOperator.TestFullReducerTestGraphs(base);
            this.builder = builder()
                    .setRules(List.of(new AttachFullReducer()))
                    .setPlanNodeIdAllocator(base.getIdAllocator())
                    .setOperatorMap(base.getOperatorMap())
                    .disablePTOptimizationTrick(true);
        }

        public Triple<Plan, List<Operator>, List<String>> physicalPlanForTestFullReducerOperator()
        {
            Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> pair = graphs.graphForTestFullReducerOperator();
            PlanBuildContext context = builder.setOrderedGraph(pair.getKey()).build();
            Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.of(new LinkedList<>(pair.getValue())));
            return Triple.of(planPair.getLeft(), planPair.getRight(), List.of(
                    "numberOfR1Assignments: 2",
                    "Domain: [red, l]",
                    "Domain: [red, a, l]"
            ));
        }

        public Triple<Plan, List<Operator>, List<String>> physicalPlan2ForTestFullReducerOperator()
        {
            Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> pair = graphs.graph2ForTestFullReducerOperator();
            PlanBuildContext context = builder.setOrderedGraph(pair.getKey()).build();
            Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.of(
                    new LinkedList<>(pair.getValue())));
            return Triple.of(planPair.getLeft(), planPair.getRight(), List.of(
                    "multiwaycomplex.TestFullReducerTestGraphs_R3 [1]\n" +
                            "multiwaycomplex.TestFullReducerTestGraphs_R4 [1]\n" +
                            "multiwaycomplex.TestFullReducerTestGraphs_R1 [1]\n" +
                            "multiwaycomplex.TestFullReducerTestGraphs_R2 [3]\n" +
                            "estimateCostOfFullReducer: 28"
            ));
        }

        /**
         * In this test case, we specifically test the code when the tuples in one relation are completely removed
         * by the full reducer.
         */
        public Triple<Plan, List<Operator>, List<String>> physicalPlan3ForTestFullReducerOperator()
        {
            Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> pair = graphs.graph3ForTestFullReducerOperator();
            PlanBuildContext context = builder.setOrderedGraph(pair.getKey()).build();
            Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.of(
                    new LinkedList<>(pair.getValue())));
            return Triple.of(planPair.getLeft(), planPair.getRight(), List.of(
                    "multiwaycomplex.physicalPlan3ForTestFullReducerOperator_R1 [2]"
            ));
        }

        /**
         * In this case, we test that underlying domain should not eliminate the duplicates. Otherwise, the join
         * result will be incorrect for this test case.
         */
        public Triple<Plan, List<Operator>, List<String>> physicalPlan4ForTestFullReducerOperator()
        {
            Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> pair = graphs.graph4ForTestFullReducerOperator();
            PlanBuildContext context = builder.setOrderedGraph(pair.getKey()).build();
            Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.of(
                    new LinkedList<>(pair.getValue())));
            return Triple.of(planPair.getLeft(), planPair.getRight(), List.of());
        }
    }

    private Object[][] testFullReducerVanillaOperatorDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestFullReducerOperator.TestFullReducerTestCases.class)));
    }

    @DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
    @ParameterizedTest
    @MethodSource("testFullReducerVanillaOperatorDataProvider")
    public void testFullReducerVaniilaOperator(Triple<Plan, List<Operator>, List<String>> triple)
    {
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
        Operator rootOperator = triple.getLeft().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("statistics:\n" + statistics);
        }
        triple.getRight().forEach(expectedMsg -> {
            if (traceLogger.isDebugEnabled()) {
                traceLogger.debug("expectedMsg:\n" + expectedMsg);
            }
            assertTrue(statistics.contains(expectedMsg));
        });
    }

    @Test
    public void testSemiJoinOrdering()
    {
        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();

        String relationT = "casefour_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        List<String> attributesT = new ArrayList<>(List.of("x"));
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue(VARCHAR, "green")),
                    Collections.singletonList(new StringValue(VARCHAR, "green")),
                    Collections.singletonList(new StringValue(VARCHAR, "red"))));
            jdbcClient.ingestRelation(schemaName, relationT, attributesT, new ArrayList<>(List.of(VARCHAR)), relationValT);
        }
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, attributesT, new MultiwayJoinDomain());

        String relationS = "casefour_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        List<String> attributeS = new ArrayList<>(Arrays.asList("x", "y", "z"));
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                    List.of(new StringValue(VARCHAR, "red"), new StringValue(VARCHAR, "1"), new StringValue(VARCHAR, "2")),
                    Arrays.asList(new StringValue(VARCHAR, "red"), new StringValue(VARCHAR, "3"), new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(schemaName, relationS, attributeS, new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)), relationValS);
        }
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, attributeS, new MultiwayJoinDomain());

        String relationB = "casefour_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        List<String> attributeB = new ArrayList<>(List.of("z"));
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(List.of(Collections.singletonList(new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(schemaName, relationB, attributeB, new ArrayList<>(List.of(VARCHAR)), relationValB);
        }
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributeB, new MultiwayJoinDomain());

        String relationR = "casefour_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        List<String> attributeR = new ArrayList<>(Arrays.asList("y", "z"));
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(Arrays.asList(new StringValue(VARCHAR, "3"), new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(schemaName, relationR, attributeR, new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)), relationValR);
        }
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributeR, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeR), asEdge(nodeS, nodeB))), nodeT);
        PlanBuildContext context = builder()
                .setRules(List.of(new AttachFullReducer()))
                .setOrderedGraph(orderedGraph)
                .setPlanNodeIdAllocator(base.getIdAllocator())
                .setOperatorMap(base.getOperatorMap())
                .disablePTOptimizationTrick(true)
                .build();
        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.empty());
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getLeft().getRoot().getOperator();
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.Yannakakis)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        YannakakisAggregateStatisticsInformation aggStatsInfo = (YannakakisAggregateStatisticsInformation) factory.get();
        // See cost-model-y.pdf
        assertEquals(6, aggStatsInfo.getSummationOfSemijoinOutputSize());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
