package org.zhu45.treetracker.relational.planner;

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
import org.zhu45.treetracker.relational.operator.*;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.rule.DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestDisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee
{
    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestDisableNGLWithoutViolatingTTJTheoreticalGuarantee";
    private StatisticsInformationPrinter printer;
    private TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBaseTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee()),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        printer = new StatisticsInformationPrinter();
    }

    @Test
    public void testSuccessfulApplicationOfRuleWhenAllColumnsAreUnique()
    {
        Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        base.testPhysicalPlanExecution(pair);
        Operator leftMostOperator = pair.getLeft().getRoot().getOperator().getPlanBuildContext().getLeftMostPlanNodeOperator();
        assertEquals(TupleBasedTableScanOperator.class, leftMostOperator.getClass());
    }

    /**
     * In the test case, artificial attribute of cast_info is not unique. But, we
     * should still use normal table scan operator because the attribute is not associated
     * with join idx of the NoGoodListMap.
     */
    @Test
    public void testSuccessfulApplicationOfRuleWhenJoinIdxColumnUnique()
    {
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "tSuccessAppOfRuleWhenJoinIdxColumnUnique";
        String relationCastInfo = relationNamePrefix + "_cast_info";
        SchemaTableName schemaTableNameCastInfo = new SchemaTableName(schemaName, relationCastInfo);
        if (jdbcClient.getTableHandle(schemaTableNameCastInfo) == null) {
            List<List<RelationalValue>> relationValCastInfo = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(1), IntegerValue.of(1), IntegerValue.of(1)),
                    List.of(IntegerValue.of(2), IntegerValue.of(2), IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationCastInfo,
                    new ArrayList<>(List.of("person_id", "movie_id", "artificial")),
                    new ArrayList<>(List.of(INTEGER, INTEGER, INTEGER)),
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
        base.testPhysicalPlanExecution(pair);
        Operator leftMostOperator = pair.getLeft().getRoot().getOperator().getPlanBuildContext().getLeftMostPlanNodeOperator();
        assertEquals(TupleBasedTableScanOperator.class, leftMostOperator.getClass());
    }

    /**
     * In this test case, (movie_id, info_type_id) of movie_info in composition are unique and they are together is
     * join idx column due to movie_info_idx
     */
    @Test
    public void testSuccessfulApplicationOfRuleWhenCompositeJoinIdxColumnsUnique()
    {
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "tSuccessAppOfRuleWhenCompJoinIdxColUnique";
        String relationMovieInfo = relationNamePrefix + "_movie_info";
        SchemaTableName schemaTableNameMovieInfo = new SchemaTableName(schemaName, relationMovieInfo);
        if (jdbcClient.getTableHandle(schemaTableNameMovieInfo) == null) {
            List<List<RelationalValue>> relationValMovieInfo = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(1), IntegerValue.of(2)),
                    List.of(IntegerValue.of(1), IntegerValue.of(3))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationMovieInfo,
                    new ArrayList<>(List.of("movie_id", "info_type_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationValMovieInfo);
        }
        MultiwayJoinNode nodeMovieInfo = new MultiwayJoinNode(schemaTableNameMovieInfo, new MultiwayJoinDomain());

        String relationMovieInfoIdx = relationNamePrefix + "_movie_info_idx";
        SchemaTableName schemaTableNameMovieInfoIdx = new SchemaTableName(schemaName, relationMovieInfoIdx);
        if (jdbcClient.getTableHandle(schemaTableNameMovieInfoIdx) == null) {
            List<List<RelationalValue>> relationValMovieInfoIdx = List.of(
                    List.of(IntegerValue.of(1), IntegerValue.of(1)));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationMovieInfoIdx,
                    new ArrayList<>(List.of("movie_id", "info_type_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationValMovieInfoIdx);
        }
        MultiwayJoinNode nodeMovieInfoIdx = new MultiwayJoinNode(schemaTableNameMovieInfoIdx, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeMovieInfo, nodeMovieInfoIdx))), nodeMovieInfo);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameMovieInfo, schemaTableNameMovieInfoIdx))),
                Optional.empty());
        base.testPhysicalPlanExecution(pair);
        Operator leftMostOperator = pair.getLeft().getRoot().getOperator().getPlanBuildContext().getLeftMostPlanNodeOperator();
        assertEquals(TupleBasedTableScanOperator.class, leftMostOperator.getClass());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
