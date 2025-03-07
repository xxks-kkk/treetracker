package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
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
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodList;
import org.zhu45.treetracker.relational.operator.noGoodList.PlainNoGoodList;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildAllTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBaseTreeTrackerOneBetaHashTableOperator
{
    private static final String naturalJoinTable = "TestTupleBaseTreeTrackerOneBetaHashTableOperator";

    private TestingPhysicalPlanBase base;
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBaseTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(DatabaseSuppler.class.getName(), Level.INFO);
            Configurator.setAllLevels(TestingMultiwayJoinDatabaseComplex.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(ExecutionNormal.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBaseTreeTrackerOneBetaTableScanOperator.class),
                        Optional.of(TupleBaseTreeTrackerOneBetaHashTableOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    private Object[][] testTupleBaseTreeTrackerOneBetaHashTableOperatorTestCasesDataProvider()
    {
        return twoDlistTo2DArray(buildAllTestCases(base));
    }

    @ParameterizedTest
    @MethodSource("testTupleBaseTreeTrackerOneBetaHashTableOperatorTestCasesDataProvider")
    public void testTupleBaseTreeTrackerOneBetaHashTableOperator(Pair<Plan, List<Operator>> pair)
    {
        testLeftDeepQueryPlansTestCasesOnly(pair, base);
    }

    public static void testLeftDeepQueryPlansTestCasesOnly(Pair<Plan, List<Operator>> pair, TestingPhysicalPlanBase base)
    {
        Operator RootOperator = pair.getKey().getRoot().getOperator();
        if (RootOperator.getOperatorType() != OptType.join ||
                (RootOperator.getOperatorType() == OptType.join && OptType.join == ((TupleBasedJoinOperator) RootOperator).r1Operator.getOperatorType())) {
            // TTJ operator assumes left-deep query plan. Thus, we only run the test cases with left-deep plans.
            base.testPhysicalPlanExecution(pair);
        }
        else {
            pair.getValue().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
        }
    }

    private Stream<NoGoodList> testNoGoodListAndTuplesRemovedFromHashTableDataProvider()
    {
        return Stream.of(PlainNoGoodList.create());
    }

    // NOTE: Further improvement see https://gitlab.com/xxks-kkk/challenge-set/-/issues/152
    @ParameterizedTest
    @MethodSource("testNoGoodListAndTuplesRemovedFromHashTableDataProvider")
    public void testNoGoodListAndTuplesRemovedFromHashTable(NoGoodList noGoodList)
    {
        int numberOfNoGoodTuples = 10;
        int numberOfTuplesRemovedFromHashTableS = 5;

        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "testNoGoodListAndTuplesRemovedFromHashTable";
        String relationT = relationNamePrefix + "_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue("green")),
                    Collections.singletonList(new StringValue("red"))));
            for (int i = 0; i < numberOfNoGoodTuples; ++i) {
                relationValT.add(Collections.singletonList(new StringValue("green")));
            }
            relationValT.add(Collections.singletonList(new StringValue("blue")));
            relationValT.add(Collections.singletonList(new StringValue("red")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationT,
                    new ArrayList<>(List.of("x")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationValT);
        }
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        String relationS = relationNamePrefix + "_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(List.of(
                    Arrays.asList(new StringValue("red"), new StringValue("3"), new StringValue("2"))));
            for (int i = 0; i < numberOfTuplesRemovedFromHashTableS; ++i) {
                relationValS.add(Arrays.asList(new StringValue("red"), new StringValue("1"), new StringValue("2")));
            }
            jdbcClient.ingestRelation(
                    schemaName,
                    relationS,
                    new ArrayList<>(Arrays.asList("x", "y", "z")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                    relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        String relationB = relationNamePrefix + "_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue("2"))
            ));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationB,
                    new ArrayList<>(Arrays.asList("z")),
                    new ArrayList<>(Arrays.asList(VARCHAR)),
                    relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        String relationR = relationNamePrefix + "_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue("3"), new StringValue("2"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationR,
                    new ArrayList<>(Arrays.asList("y", "z")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                    relationValR);
        }
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeT, nodeS),
                asEdge(nodeS, nodeB),
                asEdge(nodeS, nodeR))), nodeT);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS, schemaTableNameB, schemaTableNameR))),
                Optional.of(noGoodList));
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertTrue(statistics.contains("numberOfNoGoodTuples: " + numberOfTuplesRemovedFromHashTableS));
        // The followings are for Table Scan.
        assertTrue(statistics.contains("numberOfNoGoodTuplesFiltered: " + numberOfNoGoodTuples));
        assertTrue(statistics.contains("numberOfNoGoodTuples: " + 2));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
