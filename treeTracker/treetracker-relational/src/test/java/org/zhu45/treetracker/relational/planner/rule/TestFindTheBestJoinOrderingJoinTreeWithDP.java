package org.zhu45.treetracker.relational.planner.rule;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TestTTJAggregateStatisticsInformationWithTTJV2Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstProvider;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProviderConfiguration;
import org.zhu45.treetracker.relational.planner.cost.TTJCardEstProvider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFindTheBestJoinOrderingJoinTreeWithDP
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TestFindTheBestJoinOrderingJoinTreeWithDP.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestFindTheBestJoinOrderingJoinTreeWithDP.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(FindTheBestJoinOrderingJoinTreeWithDP.class.getName(), Level.TRACE);
        }
    }

    // Possible test strategy:
    // 1. Use the ByteBuddy to mock (or https://www.vogella.com/tutorials/Mockito/article.html)
    // 2. Manually work out an example and compare the content of DP
    @Test
    public void test()
    {
        FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration =
                FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration.builder()
                        .enableTTJExecutionForEachCell(true)
                        .build();
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(true)
                .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(true)
                .includeInnerRelationSize(true)
                .build();
        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                TestingMultiwayJoinDatabaseComplexSupplier.get(),
                "TestFindTheBestJoinOrderingJoinTreeWithDP",
                List.of(new FindTheBestJoinOrderingJoinTreeWithDP(
                        new TTJCardEstProvider(
                                new JoinTreeCostEstProvider(config)),
                        ruleConfiguration)),
                ExecutionNormal.class,
                TestingMultiwayJoinDatabaseComplexSupplier.get().getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases cases = new TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases(base);
        Triple<Plan, List<Operator>, Integer> test = cases.physicalPlan2ForTestEstimateCostOfTTJ();
        base.testPhysicalPlanExecution(Pair.of(test.getLeft(), test.getMiddle()));
        FindTheBestJoinOrderingWithDP.DPTable dpTable = test.getLeft().getPlanStatistics().getDpTable();
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(dpTable);
        }
        // check the top most DP cell
        FindTheBestJoinOrderingWithDP.DPTableEntry topMostEntry = dpTable.getBestPlan();
        assertEquals(topMostEntry.stats.ttjSizeOfIntermediateResultsThatArePartOfFinalJoinResult,
                topMostEntry.cardEstReturn.getCostReturn().getCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult());
        assertEquals(topMostEntry.stats.ttjNumberOfDanglingTuples,
                topMostEntry.cardEstReturn.getCostReturn().getCost() -
                        topMostEntry.cardEstReturn.getCostReturn().getCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult() -
                        topMostEntry.cardEstReturn.getCostReturn().getInnerRelationCleanStateSize());
        assertEquals(topMostEntry.stats.ttjInnerRelationCleanStateSize,
                topMostEntry.cardEstReturn.getCostReturn().getInnerRelationCleanStateSize());
        // check another cell
        List<SchemaTableName> schemaTableNames = getSchemaTableNames(test.getLeft().getRoot());
        Set<SchemaTableName> entrySchemas = new HashSet<>();
        for (SchemaTableName schemaTableName : schemaTableNames) {
            String tableName = schemaTableName.getTableName();
            if (tableName.contains("T") || tableName.contains("S") || tableName.contains("R")) {
                entrySchemas.add(schemaTableName);
            }
        }
        FindTheBestJoinOrderingWithDP.DPTableEntryId id = FindTheBestJoinOrderingWithDP.DPTableEntryId.getDPTableEntryId(entrySchemas);
        FindTheBestJoinOrderingWithDP.DPTableEntry entry = dpTable.getDPEntry(id);
        assertNotNull(entry);
        assertEquals(entry.stats.ttjSizeOfIntermediateResultsThatArePartOfFinalJoinResult,
                entry.cardEstReturn.getCostReturn().getCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult());
        assertEquals(entry.stats.ttjNumberOfDanglingTuples,
                entry.cardEstReturn.getCostReturn().getCost() -
                        entry.cardEstReturn.getCostReturn().getCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult() -
                        entry.cardEstReturn.getCostReturn().getInnerRelationCleanStateSize());
        assertEquals(topMostEntry.stats.ttjInnerRelationCleanStateSize,
                topMostEntry.cardEstReturn.getCostReturn().getInnerRelationCleanStateSize());
    }

    @Test
    public void testHJPhysicalHeuristics()
    {
        FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration =
                FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration.builder()
                        .enableTTJExecutionForEachCell(false)
                        .enableHJPhysicalHeuristics(true)
                        .build();
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(true)
                .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(true)
                .includeInnerRelationSize(true)
                .build();
        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                TestingMultiwayJoinDatabaseComplexSupplier.get(),
                "TestFindTheBestJoinOrderingJoinTreeWithDP",
                List.of(new FindTheBestJoinOrderingJoinTreeWithDP(
                        new TTJCardEstProvider(
                                new JoinTreeCostEstProvider(config)),
                        ruleConfiguration)),
                ExecutionNormal.class,
                TestingMultiwayJoinDatabaseComplexSupplier.get().getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));

        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String region = this.getClass().getSimpleName() + "_region";
        SchemaTableName schemaTableNameRegion = new SchemaTableName(schemaName, region);
        List<String> regionAttr = List.of("regionkey");
        if (jdbcClient.getTableHandle(schemaTableNameRegion) == null) {
            List<List<RelationalValue>> relationValRegion = new ArrayList<>(Arrays.asList(
                    Arrays.asList(IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    region,
                    regionAttr,
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relationValRegion);
        }
        MultiwayJoinNode nodeRegion = new MultiwayJoinNode(schemaTableNameRegion, regionAttr, new MultiwayJoinDomain());

        String nation = this.getClass().getSimpleName() + "_nation";
        SchemaTableName schemaTableNameNation = new SchemaTableName(schemaName, nation);
        List<String> nationAttr = List.of("nationkey", "regionkey");
        if (jdbcClient.getTableHandle(schemaTableNameNation) == null) {
            List<List<RelationalValue>> relationValNation = new ArrayList<>(Arrays.asList(
                    Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                    Arrays.asList(IntegerValue.of(2), IntegerValue.of(3))));
            jdbcClient.ingestRelation(
                    schemaName,
                    nation,
                    nationAttr,
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                    relationValNation);
        }
        MultiwayJoinNode nodeNation = new MultiwayJoinNode(schemaTableNameNation, nationAttr, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeNation, nodeRegion))), nodeRegion);
        Pair<Plan, List<Operator>> test = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.empty(), Optional.empty());

        base.testPhysicalPlanExecution(Pair.of(test.getLeft(), test.getRight()));
        FindTheBestJoinOrderingWithDP.DPTable dpTable = test.getLeft().getPlanStatistics().getDpTable();
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(dpTable);
        }
        // check the top most DP cell
        FindTheBestJoinOrderingWithDP.DPTableEntry topMostEntry = dpTable.getBestPlan();
        assertEquals(List.of(schemaTableNameNation, schemaTableNameRegion), topMostEntry.planAssociatedWithOptCost);
    }
}
