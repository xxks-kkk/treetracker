package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSingleValueJavIntRowNoGoodListMap
{
    private static final String naturalJoinTable = "TestSingleValueJavNoGoodListMap";

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

    /**
     * Test SingleValueJavNoGoodListMap is activated for the qualified query and
     * Test SingleValueJavNoGoodListMap works as expected (e.g., filter out correct number of no-good tuples)
     * for qualified query.
     */
    @Test
    public void testActivationForQualifiedQueryAndWorkAsExpected()
    {
        int numberOfNoGoodTuplesDueToPersonIdSolely = 2;
        int numberOfNoGoodTuplesDueToMovieIdSolely = 3;
        int numberOfNoGoodTuplesDueToBothPersonIdAndMovieId = 4;

        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "testActivationForQualifiedQueryIntRow";
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
                Optional.empty());
        assertTrue(pair.getLeft().getPlanStatistics().getNoGoodListMapClazzName().contains("SingleValueJavIntRowNoGoodListMap"));
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertTrue(statistics.contains("numberOfNoGoodTuples: " + 2));
        assertTrue(statistics.contains("numberOfNoGoodTuplesFiltered: " + (numberOfNoGoodTuplesDueToPersonIdSolely
                + numberOfNoGoodTuplesDueToMovieIdSolely + numberOfNoGoodTuplesDueToBothPersonIdAndMovieId)));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
