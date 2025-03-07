package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
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
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OperatorSpecification;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodList;
import org.zhu45.treetracker.relational.operator.noGoodList.PlainNoGoodList;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.OperatorSpecification.findTargetOperator;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.TestTupleBaseTreeTrackerOneBetaHashTableOperator.testLeftDeepQueryPlansTestCasesOnly;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildAllTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedHighPerfTreeTrackerOneBetaHashTableOperator
{
    private static final String naturalJoinTable = "TestTupleBaseTreeTrackerOneBetaHashTableOperator";

    private TestingPhysicalPlanBase base;
    private StatisticsInformationPrinter printer;

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
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
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
            List<List<RelationalValue>> relationValB = List.of(Collections.singletonList(new StringValue("2")));
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
            List<List<RelationalValue>> relationValR = List.of(Arrays.asList(new StringValue("3"), new StringValue("2")));
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

    /**
     * Checking the performance enhancement about jav working properly. That is, noGoodList now contains
     * jav instead of rows. As a result, this test checks whether tuples can be filtered out based on
     * their javs instead of rows.
     */
    @ParameterizedTest
    @MethodSource("testNoGoodListAndTuplesRemovedFromHashTableDataProvider")
    public void testPerformanceEnhancementJavFiltered(NoGoodList noGoodList)
    {
        int numberOfNoGoodTuplesDueToPersonIdSolely = 2;
        int numberOfNoGoodTuplesDueToMovieIdSolely = 3;
        int numberOfNoGoodTuplesDueToBothPersonIdAndMovieId = 4;

        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "testPerformanceEnhancementJavFiltered";
        String relationCastInfo = relationNamePrefix + "_cast_info";
        SchemaTableName schemaTableNameCastInfo = new SchemaTableName(schemaName, relationCastInfo);
        if (jdbcClient.getTableHandle(schemaTableNameCastInfo) == null) {
            List<List<RelationalValue>> relationValCastInfo = new ArrayList<>(Arrays.asList(
                    List.of(new IntegerValue(1), new IntegerValue(1))));
            for (int i = 0; i < numberOfNoGoodTuplesDueToPersonIdSolely; ++i) {
                relationValCastInfo.add(
                        List.of(new IntegerValue(3), new IntegerValue(1)));
            }
            for (int i = 0; i < numberOfNoGoodTuplesDueToMovieIdSolely; ++i) {
                relationValCastInfo.add(
                        List.of(new IntegerValue(1), new IntegerValue(2)));
            }
            for (int i = 0; i < numberOfNoGoodTuplesDueToBothPersonIdAndMovieId; ++i) {
                relationValCastInfo.add(
                        List.of(new IntegerValue(3), new IntegerValue(2)));
            }
            relationValCastInfo.addAll(List.of(
                    // both "4" and "5" do not appear in the noGoodList but their corresponding tuples should be successfully filtered out.
                    List.of(new IntegerValue(3), new IntegerValue(4)),
                    List.of(new IntegerValue(5), new IntegerValue(2))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationCastInfo,
                    new ArrayList<>(List.of("person_id", "movie_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationValCastInfo);
        }
        MultiwayJoinDomain domainCastInfo = new MultiwayJoinDomain();
        MultiwayJoinNode nodeCastInfo = new MultiwayJoinNode(schemaTableNameCastInfo, domainCastInfo);

        String relationAkaName = relationNamePrefix + "_aka_name";
        SchemaTableName schemaTableNameAkaName = new SchemaTableName(schemaName, relationAkaName);
        if (jdbcClient.getTableHandle(schemaTableNameAkaName) == null) {
            List<List<RelationalValue>> relationValAkaName = List.of(
                    List.of(new IntegerValue(1)));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationAkaName,
                    new ArrayList<>(List.of("person_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValAkaName);
        }
        MultiwayJoinDomain domainAkaName = new MultiwayJoinDomain();
        MultiwayJoinNode nodeAkaName = new MultiwayJoinNode(schemaTableNameAkaName, domainAkaName);

        String relationMovieCompanies = relationNamePrefix + "_movie_companies";
        SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, relationMovieCompanies);
        if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
            List<List<RelationalValue>> relationValMovieCompanies = List.of(
                    List.of(new IntegerValue(1)));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationMovieCompanies,
                    new ArrayList<>(List.of("movie_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValMovieCompanies);
        }
        MultiwayJoinDomain domainMovieCompanies = new MultiwayJoinDomain();
        MultiwayJoinNode nodeMovieCompanies = new MultiwayJoinNode(schemaTableNameMovieCompanies, domainMovieCompanies);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeCastInfo, nodeAkaName),
                asEdge(nodeCastInfo, nodeMovieCompanies))), nodeCastInfo);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameCastInfo, schemaTableNameAkaName, schemaTableNameMovieCompanies))),
                Optional.of(noGoodList));
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertTrue(statistics.contains("numberOfNoGoodTuples: " + 2));
        assertTrue(statistics.contains("numberOfNoGoodTuplesFiltered: " + (numberOfNoGoodTuplesDueToPersonIdSolely
                + numberOfNoGoodTuplesDueToMovieIdSolely + numberOfNoGoodTuplesDueToBothPersonIdAndMovieId)));
    }

    /**
     * Per Remy:
     * Also, in the pseudocode for the arxiv version deleteDT removes the matching tuple both from the hash table and the vector of matching tuples (line 24 Algorithm 3.1).
     * I think it's only necessary to remove from the hash table, since you won't ever access the same tuple again in the vector.
     * This could make deleteDT faster.
     * <p>
     * This test case verifies that in our implementation, due to referencing, removal from MatchingTuples also means removal from
     * the underlying hash table at the same time. Specifically, in our implementation, hash table is implemented as
     * {@code
     * Map<JoinValueContainerKey, List<Row>>
     * }
     * and MatchingTuples from the algorithm is implemented as
     * {@code
     * l = hashTableH.get(jav);
     * }
     * Then, we initialize an iterator on {@code l} as {@code iL = l.iterator();} and the tuple deletion in deleteDT() is implemented as
     * {@code iL.remove();}. By the semantics of iterator's {@code remove()}, tuple is removed from {@code l}. Since {@code l} contains
     * a collection of references to the original tuples in the hash table, deleting reference in {@code l} also means deleting the tuple
     * in the hash table.
     */
    @Test
    public void testRemoveMatchingTuplesAndHashTable()
    {
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        var pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseEleven();
        base.testPhysicalPlanExecution(pair);
        OperatorSpecification leftOperator = OperatorSpecification.builder()
                .optType(OptType.table)
                .relationName("rmMatchTest_A")
                .build();
        OperatorSpecification rightOperator = OperatorSpecification.builder()
                .optType(OptType.table)
                .relationName("rmMatchTest_B")
                .build();
        OperatorSpecification rootOperator = OperatorSpecification.builder()
                .optType(OptType.join)
                .children(List.of(leftOperator, rightOperator))
                .build();
        Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
        assertEquals(1, targetOperator.getStatisticsInformation().getHashTableSizeAfterEvaluation());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
