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
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

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
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFullReducerOperator
{
    private Logger traceLogger = LogManager.getLogger(TestFullReducerOperator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestFullReducerOperator";
    private StatisticsInformationPrinter printer;
    private TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(FullReducerOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedLeftSemiHashJoinOperator.class.getName(), Level.TRACE);
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

    private Object[][] testFullReducerOperatorDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestFullReducerTestCases.class)));
    }

    @DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
    @ParameterizedTest
    @MethodSource("testFullReducerOperatorDataProvider")
    public void testFullReducerOperator(Triple<Plan, List<Operator>, List<String>> triple)
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

    public static class TestFullReducerTestGraphs
            implements TestCases
    {
        private final String schemaName;
        private final JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestFullReducerTestGraphs(TestingPhysicalPlanBase base)
        {
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> graphForTestFullReducerOperator()
        {
            String relationT = this.getClass().getSimpleName() + "_T";
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
            if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
                List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("red"), new StringValue("l")),
                        Arrays.asList(new StringValue("blue"), new StringValue("l")),
                        Arrays.asList(new StringValue("red"), new StringValue("x")),
                        Arrays.asList(new StringValue("yellow"), new StringValue("y"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationT,
                        new ArrayList<>(Arrays.asList("color", "letter2")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                        relationValT);
            }
            MultiwayJoinDomain domainT = new MultiwayJoinDomain();
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

            String relationS = this.getClass().getSimpleName() + "_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("red"), new StringValue("a"), new StringValue("l")),
                        Arrays.asList(new StringValue("black"), new StringValue("b"), new StringValue("l"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationS,
                        new ArrayList<>(Arrays.asList("color", "letter1", "letter2")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                        relationValS);
            }
            MultiwayJoinDomain domainS = new MultiwayJoinDomain();
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

            return Pair.of(getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                            asEdge(nodeT, nodeS))), nodeT),
                    List.of(schemaTableNameT, schemaTableNameS));
        }

        public Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> graph2ForTestFullReducerOperator()
        {
            String relationR1 = this.getClass().getSimpleName() + "_R1";
            SchemaTableName schemaTableNameR1 = new SchemaTableName(schemaName, relationR1);
            if (jdbcClient.getTableHandle(schemaTableNameR1) == null) {
                List<List<RelationalValue>> relationValR1 = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("1"), new StringValue("20")),
                        Arrays.asList(new StringValue("1"), new StringValue("10")),
                        Arrays.asList(new StringValue("4"), new StringValue("60"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR1,
                        new ArrayList<>(Arrays.asList("A1", "A2")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                        relationValR1);
            }
            MultiwayJoinDomain domainR1 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR1 = new MultiwayJoinNode(schemaTableNameR1, domainR1);

            String relationR2 = this.getClass().getSimpleName() + "_R2";
            SchemaTableName schemaTableNameR2 = new SchemaTableName(schemaName, relationR2);
            if (jdbcClient.getTableHandle(schemaTableNameR2) == null) {
                List<List<RelationalValue>> relationValR2 = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("1"), new StringValue("10"), new StringValue("100")),
                        Arrays.asList(new StringValue("1"), new StringValue("20"), new StringValue("100")),
                        Arrays.asList(new StringValue("3"), new StringValue("10"), new StringValue("300")),
                        Arrays.asList(new StringValue("1"), new StringValue("40"), new StringValue("300")),
                        Arrays.asList(new StringValue("2"), new StringValue("30"), new StringValue("200"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR2,
                        new ArrayList<>(Arrays.asList("A1", "A2", "A3")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                        relationValR2);
            }
            MultiwayJoinDomain domainR2 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR2 = new MultiwayJoinNode(schemaTableNameR2, domainR2);

            String relationR3 = this.getClass().getSimpleName() + "_R3";
            SchemaTableName schemaTableNameR3 = new SchemaTableName(schemaName, relationR3);
            if (jdbcClient.getTableHandle(schemaTableNameR3) == null) {
                List<List<RelationalValue>> relationValR1 = new ArrayList<>(Arrays.asList(
                        List.of(new StringValue("10")),
                        List.of(new StringValue("20")),
                        List.of(new StringValue("30"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR3,
                        new ArrayList<>(List.of("A2")),
                        new ArrayList<>(List.of(VARCHAR)),
                        relationValR1);
            }
            MultiwayJoinDomain domainR3 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR3 = new MultiwayJoinNode(schemaTableNameR3, domainR3);

            String relationR4 = this.getClass().getSimpleName() + "_R4";
            SchemaTableName schemaTableNameR4 = new SchemaTableName(schemaName, relationR4);
            if (jdbcClient.getTableHandle(schemaTableNameR4) == null) {
                List<List<RelationalValue>> relationValR4 = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("1"), new StringValue("10"), new StringValue("1000")),
                        Arrays.asList(new StringValue("1"), new StringValue("20"), new StringValue("1000")),
                        Arrays.asList(new StringValue("1"), new StringValue("20"), new StringValue("2000")),
                        Arrays.asList(new StringValue("2"), new StringValue("20"), new StringValue("2000"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR4,
                        new ArrayList<>(Arrays.asList("A1", "A2", "A4")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                        relationValR4);
            }
            MultiwayJoinDomain domainR4 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR4 = new MultiwayJoinNode(schemaTableNameR4, domainR4);

            return Pair.of(getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                            asEdge(nodeR1, nodeR2),
                            asEdge(nodeR2, nodeR3),
                            asEdge(nodeR2, nodeR4))), nodeR1),
                    List.of(schemaTableNameR1, schemaTableNameR2, schemaTableNameR3, schemaTableNameR4));
        }

        /**
         * In this test case, we specifically test the code when the tuples in one relation are completely removed
         * by the full reducer.
         */
        public Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> graph3ForTestFullReducerOperator()
        {
            String relationPrefix = "physicalPlan3ForTestFullReducerOperator";
            String relationR1 = relationPrefix + "_R1";
            SchemaTableName schemaTableNameR1 = new SchemaTableName(schemaName, relationR1);
            if (jdbcClient.getTableHandle(schemaTableNameR1) == null) {
                List<List<RelationalValue>> relationValR1 = new ArrayList<>(Arrays.asList(
                        List.of(new IntegerValue(5)),
                        List.of(new IntegerValue(6))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR1,
                        new ArrayList<>(List.of("A1")),
                        new ArrayList<>(List.of(INTEGER)),
                        relationValR1);
            }
            MultiwayJoinDomain domainR1 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR1 = new MultiwayJoinNode(schemaTableNameR1, domainR1);

            String relationR2 = relationPrefix + "_R2";
            SchemaTableName schemaTableNameR2 = new SchemaTableName(schemaName, relationR2);
            if (jdbcClient.getTableHandle(schemaTableNameR2) == null) {
                List<List<RelationalValue>> relationValR2 = new ArrayList<>(Arrays.asList(
                        List.of(new IntegerValue(4))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR2,
                        new ArrayList<>(List.of("A1")),
                        new ArrayList<>(List.of(INTEGER)),
                        relationValR2);
            }
            MultiwayJoinDomain domainR2 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR2 = new MultiwayJoinNode(schemaTableNameR2, domainR2);

            return Pair.of(getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                            asEdge(nodeR1, nodeR2))), nodeR1),
                    List.of(schemaTableNameR1, schemaTableNameR2));
        }

        /**
         * In this case, we test that underlying domain should not eliminate the duplicates. Otherwise, the join
         * result will be incorrect for this test case.
         */
        public Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> graph4ForTestFullReducerOperator()
        {
            String relationPrefix = "physicalPlan4ForTestFullReducerOperator";
            String relationR1 = relationPrefix + "_R1";
            SchemaTableName schemaTableNameR1 = new SchemaTableName(schemaName, relationR1);
            if (jdbcClient.getTableHandle(schemaTableNameR1) == null) {
                List<List<RelationalValue>> relationValR1 = new ArrayList<>(Arrays.asList(
                        List.of(new IntegerValue(5)),
                        List.of(new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR1,
                        new ArrayList<>(List.of("A1")),
                        new ArrayList<>(List.of(INTEGER)),
                        relationValR1);
            }
            MultiwayJoinDomain domainR1 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR1 = new MultiwayJoinNode(schemaTableNameR1, domainR1);

            String relationR2 = relationPrefix + "_R2";
            SchemaTableName schemaTableNameR2 = new SchemaTableName(schemaName, relationR2);
            if (jdbcClient.getTableHandle(schemaTableNameR2) == null) {
                List<List<RelationalValue>> relationValR2 = new ArrayList<>(Arrays.asList(
                        List.of(new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR2,
                        new ArrayList<>(List.of("A1")),
                        new ArrayList<>(List.of(INTEGER)),
                        relationValR2);
            }
            MultiwayJoinDomain domainR2 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR2 = new MultiwayJoinNode(schemaTableNameR2, domainR2);

            return Pair.of(getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                            asEdge(nodeR1, nodeR2))), nodeR1),
                    List.of(schemaTableNameR1, schemaTableNameR2));
        }
    }

    public static class TestFullReducerTestCases
            implements TestCases
    {
        private TestingPhysicalPlanBase base;
        private PlanBuildContext.Builder builder;
        private TestFullReducerTestGraphs graphs;

        public TestFullReducerTestCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
            this.graphs = new TestFullReducerTestGraphs(base);
            this.builder = builder()
                    .setRules(List.of(new AttachFullReducer()))
                    .setPlanNodeIdAllocator(base.getIdAllocator())
                    .setOperatorMap(base.getOperatorMap());
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
                    "multiwaycomplex.TestFullReducerTestGraphs_R3 [1]",
                    "multiwaycomplex.TestFullReducerTestGraphs_R4 [1]",
                    "multiwaycomplex.TestFullReducerTestGraphs_R1 [1]",
                    "multiwaycomplex.TestFullReducerTestGraphs_R2 [3]",
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

    public static class TestSemiJoinOrderingTestGraphs
            implements TestCases
    {
        private final String schemaName;
        private final JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestSemiJoinOrderingTestGraphs(TestingPhysicalPlanBase base)
        {
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public Pair<SemiJoinOrdering, Integer> graph()
        {
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

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeB), asEdge(nodeS, nodeR))), nodeT);

            SemiJoinOrdering semiJoinOrdering = new SemiJoinOrdering(
                    List.of(Pair.of(nodeS, nodeR), Pair.of(nodeS, nodeB), Pair.of(nodeT, nodeS)),
                    orderedGraph);
            // See cost-model-y.pdf
            return Pair.of(semiJoinOrdering, 5);
        }

        /*
         * tpch.q8w_region(regionkey)
         * |tpch.q8w_nation(nationkey,regionkey)
         * ||tpch_int.customer(custkey,nationkey)
         * |||tpch.q8w_orders(orderkey,custkey)
         * ||||tpch_int.lineitem(orderkey,partkey,suppkey)
         * |||||tpch.q8w_part(partkey)
         * |||||tpch.q8w_supplier(suppkey,nationkey2)
         * ||||||tpch.q8w_nation2(nationkey2,regionkey2)
         */
        public Pair<SemiJoinOrdering, Integer> graph2()
        {
            String region = "region";
            SchemaTableName schemaTableNameRegion = new SchemaTableName(schemaName, region);
            List<String> attributesRegion = List.of("regionkey");
            if (jdbcClient.getTableHandle(schemaTableNameRegion) == null) {
                List<List<RelationalValue>> relationValRegion = new ArrayList<>(Arrays.asList(
                        Collections.singletonList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, region, attributesRegion, new ArrayList<>(List.of(INTEGER)), relationValRegion);
            }
            MultiwayJoinNode nodeRegion = new MultiwayJoinNode(schemaTableNameRegion, attributesRegion, new MultiwayJoinDomain());

            String nation = "nation";
            SchemaTableName schemaTableNameNation = new SchemaTableName(schemaName, nation);
            List<String> attributesNation = List.of("nationkey", "regionkey");
            if (jdbcClient.getTableHandle(schemaTableNameNation) == null) {
                List<List<RelationalValue>> relationValNation = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, nation, attributesNation, new ArrayList<>(List.of(INTEGER, INTEGER)), relationValNation);
            }
            MultiwayJoinNode nodeNation = new MultiwayJoinNode(schemaTableNameNation, attributesNation, new MultiwayJoinDomain());

            String customer = "customer";
            SchemaTableName schemaTableNameCustomer = new SchemaTableName(schemaName, customer);
            List<String> attributesCustomer = List.of("custkey", "nationkey");
            if (jdbcClient.getTableHandle(schemaTableNameCustomer) == null) {
                List<List<RelationalValue>> relationValCustomer = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, customer, attributesCustomer, new ArrayList<>(List.of(INTEGER, INTEGER)), relationValCustomer);
            }
            MultiwayJoinNode nodeCustomer = new MultiwayJoinNode(schemaTableNameCustomer, attributesCustomer, new MultiwayJoinDomain());

            String orders = "orders";
            SchemaTableName schemaTableNameOrders = new SchemaTableName(schemaName, orders);
            List<String> attributesOrders = List.of("orderkey", "custkey");
            if (jdbcClient.getTableHandle(schemaTableNameOrders) == null) {
                List<List<RelationalValue>> relationValOrders = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, orders, attributesOrders, new ArrayList<>(List.of(INTEGER, INTEGER)), relationValOrders);
            }
            MultiwayJoinNode nodeOrders = new MultiwayJoinNode(schemaTableNameOrders, attributesOrders, new MultiwayJoinDomain());

            String lineitem = "lineitem";
            SchemaTableName schemaTableNameLineItem = new SchemaTableName(schemaName, lineitem);
            List<String> attributesLineItem = List.of("orderkey", "partkey", "suppkey");
            if (jdbcClient.getTableHandle(schemaTableNameLineItem) == null) {
                List<List<RelationalValue>> relationValLineItem = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(3))));
                jdbcClient.ingestRelation(schemaName, lineitem, attributesLineItem, new ArrayList<>(List.of(INTEGER, INTEGER, INTEGER)), relationValLineItem);
            }
            MultiwayJoinNode nodeLineItem = new MultiwayJoinNode(schemaTableNameLineItem, attributesLineItem, new MultiwayJoinDomain());

            String part = "part";
            SchemaTableName schemaTableNamePart = new SchemaTableName(schemaName, part);
            List<String> attributesPart = List.of("partkey");
            if (jdbcClient.getTableHandle(schemaTableNamePart) == null) {
                List<List<RelationalValue>> relationValPart = new ArrayList<>(Arrays.asList(
                        Collections.singletonList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, part, attributesPart, new ArrayList<>(List.of(INTEGER)), relationValPart);
            }
            MultiwayJoinNode nodePart = new MultiwayJoinNode(schemaTableNamePart, attributesPart, new MultiwayJoinDomain());

            String supplier = "supplier";
            SchemaTableName schemaTableNameSupplier = new SchemaTableName(schemaName, supplier);
            List<String> attributesSupplier = List.of("suppkey", "nationkey2");
            if (jdbcClient.getTableHandle(schemaTableNameSupplier) == null) {
                List<List<RelationalValue>> relationValSupplier = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, supplier, attributesSupplier, new ArrayList<>(List.of(INTEGER, INTEGER)), relationValSupplier);
            }
            MultiwayJoinNode nodeSupplier = new MultiwayJoinNode(schemaTableNameSupplier, attributesSupplier, new MultiwayJoinDomain());

            String nation2 = "nation2";
            SchemaTableName schemaTableNameNation2 = new SchemaTableName(schemaName, nation2);
            List<String> attributesNation2 = List.of("nationkey2", "regionkey2");
            if (jdbcClient.getTableHandle(schemaTableNameNation2) == null) {
                List<List<RelationalValue>> relationValNation2 = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, nation2, attributesNation2, new ArrayList<>(List.of(INTEGER, INTEGER)), relationValNation2);
            }
            MultiwayJoinNode nodeNation2 = new MultiwayJoinNode(schemaTableNameNation2, attributesNation2, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeRegion, nodeNation),
                    asEdge(nodeNation, nodeCustomer),
                    asEdge(nodeCustomer, nodeOrders),
                    asEdge(nodeOrders, nodeLineItem),
                    asEdge(nodeLineItem, nodePart),
                    asEdge(nodeLineItem, nodeSupplier),
                    asEdge(nodeSupplier, nodeNation2))), nodeRegion);

            SemiJoinOrdering semiJoinOrdering = new SemiJoinOrdering(
                    List.of(Pair.of(nodeSupplier, nodeNation2),
                            Pair.of(nodeLineItem, nodePart),
                            Pair.of(nodeLineItem, nodeSupplier),
                            Pair.of(nodeOrders, nodeLineItem),
                            Pair.of(nodeCustomer, nodeOrders),
                            Pair.of(nodeNation, nodeCustomer),
                            Pair.of(nodeRegion, nodeNation)),
                    orderedGraph);
            return Pair.of(semiJoinOrdering, null);
        }
    }

    private Object[][] testSemiJoinOrderingDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestSemiJoinOrderingTestGraphs.class)));
    }

    @ParameterizedTest
    @MethodSource("testSemiJoinOrderingDataProvider")
    public void testSemiJoinOrdering(Pair<SemiJoinOrdering, Integer> pair)
    {
        Pair<Plan, List<Operator>> planPair = base.createPhysicalPlanForYannakakis(pair.getKey());
        base.testPhysicalPlanExecution(planPair);
        Operator rootOperator = planPair.getLeft().getRoot().getOperator();
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.Yannakakis)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        YannakakisAggregateStatisticsInformation aggStatsInfo = (YannakakisAggregateStatisticsInformation) factory.get();
        if (pair.getValue() != null) {
            assertEquals((int) pair.getValue(), aggStatsInfo.getSummationOfSemijoinOutputSize());
        }
        compareSemiJoinOrdering(pair.getKey().getBottomUpPass(), aggStatsInfo.getBottomUpSemiJoinOrdering());
    }

    public static void compareSemiJoinOrdering(List<Pair<MultiwayJoinNode, MultiwayJoinNode>> semiJoinOrdering,
                                               List<List<MultiwayJoinNode>> actualSemiJoinOrdering)
    {
        List<Pair<MultiwayJoinNode, MultiwayJoinNode>> actualSemiJoinOrderPairs = new ArrayList<>();
        for (List<MultiwayJoinNode> semijoin : actualSemiJoinOrdering) {
            if (semijoin.size() == 2) {
                actualSemiJoinOrderPairs.add(Pair.of(semijoin.get(0), semijoin.get(1)));
            }
            else {
                for (int j = 1; j < semijoin.size(); ++j) {
                    actualSemiJoinOrderPairs.add(Pair.of(semijoin.get(0), semijoin.get(j)));
                }
            }
        }
        assertEquals(actualSemiJoinOrderPairs.size(), semiJoinOrdering.size());
        for (int i = 0; i < actualSemiJoinOrderPairs.size(); ++i) {
            // The nodes are get copied and connected() are changed, e.g., when enableJoinGraphHeuristicFromPT is true. Thus, directly comparing Nodes may not work.
            Pair<MultiwayJoinNode, MultiwayJoinNode> expectedPair = semiJoinOrdering.get(i);
            Pair<MultiwayJoinNode, MultiwayJoinNode> actualPair = actualSemiJoinOrderPairs.get(i);
            assertEquals(expectedPair.getKey().getSchemaTableName(), actualPair.getKey().getSchemaTableName(),
                    String.format("expectedPair: %s\nactualPair:%s", expectedPair, actualPair));
            assertEquals(expectedPair.getValue().getSchemaTableName(), actualPair.getValue().getSchemaTableName(),
                    String.format("expectedPair: %s\nactualPair:%s", expectedPair, actualPair));
        }
    }

    /**
     * This test mimics benchmark scenario where we create query instance once but run it multiple times.
     * We want to ensure that each time, the correct query result is computed.
     */
    @DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
    @ParameterizedTest
    @MethodSource("testFullReducerOperatorDataProvider")
    public void testSameQueryInstanceCanBeExecutedMultipleTimes(Triple<Plan, List<Operator>, List<String>> triple)
    {
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
