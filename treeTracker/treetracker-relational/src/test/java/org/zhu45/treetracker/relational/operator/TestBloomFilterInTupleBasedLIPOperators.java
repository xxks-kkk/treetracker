package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
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
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

/**
 * This tests out the Bloom filters in TupleBasedLIPHashJoinOperator
 * and TupleBasedLIPTableScanOperator.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestBloomFilterInTupleBasedLIPOperators
{
    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestBloomFilterInTupleBasedLIPOperators";
    private StatisticsInformationPrinter printer;

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
                Collections.emptyList(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedLIPTableScanOperator.class), Optional.of(TupleBasedLIPHashJoinOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    private Pair<Plan, List<Operator>> physicalPlanForTestBloomFilterInTupleBasedLIPOperators(TestingPhysicalPlanBase base)
    {
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();

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

        String relationS = this.getClass().getSimpleName() + "S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue("red"), new StringValue("a"), new StringValue("l"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationS,
                    new ArrayList<>(Arrays.asList("color", "letter1", "letter2")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                    relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        List<Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(nodeT, nodeS)
        ));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, nodeT);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS))),
                Optional.empty());
    }

    @Test
    public void testBloomFilterInTupleBasedLIPOperators()
    {
        Pair<Plan, List<Operator>> pair = physicalPlanForTestBloomFilterInTupleBasedLIPOperators(base);
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertTrue(statistics.contains("LIP Hash join operator (2: join2): \n" +
                "approxNumberOfTuplesInBloomFilter: 1"));
        assertTrue(statistics.contains("LIP Table scan operator (0: TestBloomFilterInTupleBasedLIPOperators_T): \n" +
                "numberOfTuplesFilteredOutByBloomFilters: 3\n" +
                "numberOfBloomFiltersRegistered: 1"));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
