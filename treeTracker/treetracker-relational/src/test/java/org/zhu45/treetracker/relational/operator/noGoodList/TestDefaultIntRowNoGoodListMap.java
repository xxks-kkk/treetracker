package org.zhu45.treetracker.relational.operator.noGoodList;

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
import org.zhu45.treetracker.relational.operator.DatabaseSuppler;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
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
public class TestDefaultIntRowNoGoodListMap
{
    private static final String naturalJoinTable = "testDefaultIntRownoGoodListMap";

    private TestingPhysicalPlanBase base;
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class.getName(), Level.TRACE);
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
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    /**
     * Test DefaultIntRowNoGoodListMap is activated for the qualified query and
     * Test DefaultIntRowNoGoodListMap works as expected (e.g., filter out correct number of no-good tuples)
     * for qualified query.
     */
    @Test
    public void testActivationForQualifiedQueryAndWorkAsExpected()
    {
        int numberOfNoGoodTuplesDueToPartSuppSolely = 2;

        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "testActivationForQualifiedQuery";

        String relationLineItem = relationNamePrefix + "_lineitem";
        SchemaTableName schemaTableNameLineItem = new SchemaTableName(schemaName, relationLineItem);
        if (jdbcClient.getTableHandle(schemaTableNameLineItem) == null) {
            List<List<RelationalValue>> relationValLineItem = new ArrayList<>(Arrays.asList(
                    List.of(new IntegerValue(1), new IntegerValue(1))));
            for (int i = 0; i < numberOfNoGoodTuplesDueToPartSuppSolely; ++i) {
                relationValLineItem.add(
                        List.of(new IntegerValue(3), new IntegerValue(1)));
            }
            jdbcClient.ingestRelation(
                    schemaName,
                    relationLineItem,
                    new ArrayList<>(List.of("partkey", "suppkey")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationValLineItem);
        }
        MultiwayJoinNode nodeLineItem = new MultiwayJoinNode(schemaTableNameLineItem, new MultiwayJoinDomain());

        String relationPartSupp = relationNamePrefix + "_partsupp";
        SchemaTableName schemaTableNamePartSupp = new SchemaTableName(schemaName, relationPartSupp);
        if (jdbcClient.getTableHandle(schemaTableNamePartSupp) == null) {
            List<List<RelationalValue>> relationValPartSupp = List.of(
                    List.of(IntegerValue.of(1), IntegerValue.of(1)));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationPartSupp,
                    new ArrayList<>(List.of("partkey", "suppkey")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationValPartSupp);
        }
        MultiwayJoinNode nodePartSupp = new MultiwayJoinNode(schemaTableNamePartSupp, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeLineItem, nodePartSupp))), nodeLineItem);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameLineItem, schemaTableNamePartSupp))),
                Optional.empty());
        assertTrue(pair.getLeft().getPlanStatistics().getNoGoodListMapClazzName().contains("DefaultIntRowNoGoodListMap"));
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertTrue(statistics.contains("numberOfNoGoodTuples: " + 1));
        assertTrue(statistics.contains("numberOfNoGoodTuplesFiltered: " + (numberOfNoGoodTuplesDueToPartSuppSolely
                - 1)));
    }

    /**
     * In this case, we test the activation due to number of join attributes between R_k and one of its child is not 1.
     */
    @Test
    public void testActivationForQualifiedQueryAndWorkAsExpected2()
    {
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "testDeactivationForUnqualifiedQuery2";
        String relationCastInfo = relationNamePrefix + "_R";
        SchemaTableName schemaTableNameCastInfo = new SchemaTableName(schemaName, relationCastInfo);
        if (jdbcClient.getTableHandle(schemaTableNameCastInfo) == null) {
            List<List<RelationalValue>> relationValCastInfo = new ArrayList<>(Arrays.asList(
                    List.of(new IntegerValue(1), new IntegerValue(1), new IntegerValue(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationCastInfo,
                    new ArrayList<>(List.of("x", "y", "z")),
                    new ArrayList<>(List.of(INTEGER, INTEGER, INTEGER)),
                    relationValCastInfo);
        }
        MultiwayJoinDomain domainCastInfo = new MultiwayJoinDomain();
        MultiwayJoinNode nodeCastInfo = new MultiwayJoinNode(schemaTableNameCastInfo, domainCastInfo);

        String relationAkaName = relationNamePrefix + "_S";
        SchemaTableName schemaTableNameAkaName = new SchemaTableName(schemaName, relationAkaName);
        if (jdbcClient.getTableHandle(schemaTableNameAkaName) == null) {
            List<List<RelationalValue>> relationValAkaName = List.of(
                    List.of(new IntegerValue(1), new IntegerValue(1)));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationAkaName,
                    new ArrayList<>(List.of("x", "y")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationValAkaName);
        }
        MultiwayJoinDomain domainAkaName = new MultiwayJoinDomain();
        MultiwayJoinNode nodeAkaName = new MultiwayJoinNode(schemaTableNameAkaName, domainAkaName);

        String relationMovieCompanies = relationNamePrefix + "_T";
        SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, relationMovieCompanies);
        if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
            List<List<RelationalValue>> relationValMovieCompanies = List.of(
                    List.of(new IntegerValue(1)));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationMovieCompanies,
                    new ArrayList<>(List.of("z")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValMovieCompanies);
        }
        MultiwayJoinDomain domainMovieCompanies = new MultiwayJoinDomain();
        MultiwayJoinNode nodeMovieCompanies = new MultiwayJoinNode(schemaTableNameMovieCompanies, domainMovieCompanies);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeCastInfo, nodeAkaName),
                asEdge(nodeCastInfo, nodeMovieCompanies))), nodeCastInfo);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                Optional.empty(),
                Optional.empty());
        assertTrue(pair.getLeft().getPlanStatistics().getNoGoodListMapClazzName().contains(DefaultIntRowNoGoodListMap.class.getCanonicalName()));
        base.testPhysicalPlanExecution(pair);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
