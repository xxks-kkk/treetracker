package org.zhu45.treetracker.relational.planner;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.DatabaseSuppler;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.planner.cost.CardEstContext;
import org.zhu45.treetracker.relational.planner.cost.CardEstProvider;
import org.zhu45.treetracker.relational.planner.cost.CardEstReturn;
import org.zhu45.treetracker.relational.planner.cost.CardEstType;
import org.zhu45.treetracker.relational.planner.cost.PostgresCardEstProvider;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingJoinTreeWithDP;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFindTheBestJoinOrderingWithDP
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TestFindTheBestJoinOrderingWithDP.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private static final String naturalJoinTable = "TestFindTheBestJoinOrderingWithDP";
    TestingMultiwayJoinDatabaseComplex database;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(DatabaseSuppler.class.getName(), Level.INFO);
            Configurator.setAllLevels(TestingMultiwayJoinDatabaseComplex.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(FindTheBestJoinOrderingWithDP.class.getName(), Level.TRACE);
        }
        database = TestingMultiwayJoinDatabaseComplexSupplier.get();
    }

    /**
     * In this test, we test the rule using an adaption of Example 16.33 from
     * "Database Systems: The Complete Book". The original example considers
     * cross product in DP.
     */
    @Test
    public void testDPUsingCompleteBookExample()
    {
        String schema = database.getSchemaName();
        String relationPrefix = "testDPUsingCompleteBookExample_";
        SchemaTableName schemaTableNameR = new SchemaTableName(schema, relationPrefix + "R");
        SchemaTableName schemaTableNameS = new SchemaTableName(schema, relationPrefix + "S");
        SchemaTableName schemaTableNameT = new SchemaTableName(schema, relationPrefix + "T");
        SchemaTableName schemaTableNameU = new SchemaTableName(schema, relationPrefix + "U");

        class CompleteExampleCostProvider
                implements CardEstProvider
        {

            @Override
            public CardEstReturn getSize(Set<SchemaTableName> schemaTableNames,
                                         CardEstContext context)
            {
                if (schemaTableNames.equals(Set.of(schemaTableNameR))) {
                    return CardEstReturn.builder(1000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameS))) {
                    return CardEstReturn.builder(1000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameT))) {
                    return CardEstReturn.builder(1000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameU))) {
                    return CardEstReturn.builder(1000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameR, schemaTableNameS))) {
                    return CardEstReturn.builder(5000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameR, schemaTableNameU))) {
                    return CardEstReturn.builder(10000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameS, schemaTableNameT))) {
                    return CardEstReturn.builder(2000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameT, schemaTableNameU))) {
                    return CardEstReturn.builder(1000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameR, schemaTableNameS, schemaTableNameT))) {
                    return CardEstReturn.builder(10000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameR, schemaTableNameS, schemaTableNameU))) {
                    return CardEstReturn.builder(50000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameR, schemaTableNameT, schemaTableNameU))) {
                    return CardEstReturn.builder(10000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameS, schemaTableNameT, schemaTableNameU))) {
                    return CardEstReturn.builder(2000).build();
                }
                if (schemaTableNames.equals(Set.of(schemaTableNameR, schemaTableNameS, schemaTableNameT, schemaTableNameU))) {
                    // According to "Database Systems: The Complete Book" (p.820), we ignore
                    // the join output size of the full set of relations
                    return CardEstReturn.builder(0).build();
                }
                // Possible if cross product is not correctly identified
                throw new RuntimeException("Unsupported input: " + schemaTableNames);
            }

            @Override
            public CardEstType getCardEstType()
            {
                return CardEstType.NONTTJ;
            }

            @Override
            public boolean isUseTrueCard()
            {
                return false;
            }
        }

        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new FindTheBestJoinOrderingWithDP(new CompleteExampleCostProvider())),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.empty(), Optional.of(TupleBasedHashJoinOperator.class))));

        JdbcClient jdbcClient = database.getJdbcClient();
        List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                List.of(IntegerValue.of(0), IntegerValue.of(0))
        ));
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNameR.getSchemaName(),
                    schemaTableNameR.getTableName(),
                    new ArrayList<>(List.of("a", "b")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationVal);
        }
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNameS.getSchemaName(),
                    schemaTableNameS.getTableName(),
                    new ArrayList<>(List.of("b", "c")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationVal);
        }
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNameT.getSchemaName(),
                    schemaTableNameT.getTableName(),
                    new ArrayList<>(List.of("c", "d")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationVal);
        }
        if (jdbcClient.getTableHandle(schemaTableNameU) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNameU.getSchemaName(),
                    schemaTableNameU.getTableName(),
                    new ArrayList<>(List.of("d", "a")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationVal);
        }
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromJoinOrdering(new JoinOrdering(List.of(schemaTableNameR, schemaTableNameS, schemaTableNameT, schemaTableNameU)));
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            System.out.println(pair.getKey().getPlanStatistics().getDpTable().toString());
        }
        List<SchemaTableName> expectedSchemaTableNameOrdering = List.of(schemaTableNameT, schemaTableNameU, schemaTableNameS, schemaTableNameR);
        assertTrue(base.getCaseVerifier().visitPlan(pair.getKey().getRoot(),
                new LinkedList<>(expectedSchemaTableNameOrdering)));
        PlanStatistics planStatistics = pair.getKey().getPlanStatistics();
        assertEquals(0, Double.compare(planStatistics.getCost(), 3000f));
        assertEquals(new JoinOrdering(expectedSchemaTableNameOrdering), planStatistics.getOptimalJoinOrdering());
        base.testPhysicalPlanExecution(pair);
    }

    /**
     * This test case effectively tests checkIfCrossProductsFreeOrderingExists, where
     * the input relation doesn't permit at least one cross-product free ordering.
     */
    @Test
    public void testDPWhenThereIsCrossProduct()
    {
        class DummyCostProvider
                implements CardEstProvider
        {
            @Override
            public CardEstReturn getSize(Set<SchemaTableName> schemaTableNames,
                                         CardEstContext context)
            {
                return CardEstReturn.builder(0).build();
            }

            @Override
            public CardEstType getCardEstType()
            {
                return CardEstType.NONTTJ;
            }

            @Override
            public boolean isUseTrueCard()
            {
                return false;
            }
        }

        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new FindTheBestJoinOrderingWithDP(new DummyCostProvider())),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.empty(), Optional.of(TupleBasedHashJoinOperator.class))));

        String schema = database.getSchemaName();
        String relationPrefix = "testDPWhenThereIsCrossProduct_";
        SchemaTableName schemaTableNameMovieLink = new SchemaTableName(schema, relationPrefix + "ml");
        SchemaTableName schemaTableNamePersonInfo = new SchemaTableName(schema, relationPrefix + "pi");
        SchemaTableName schemaTableNameInfoType = new SchemaTableName(schema, relationPrefix + "it");
        SchemaTableName schemaTableNameTitle = new SchemaTableName(schema, relationPrefix + "t");

        JdbcClient jdbcClient = database.getJdbcClient();
        List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                List.of(IntegerValue.of(0))
        ));
        if (jdbcClient.getTableHandle(schemaTableNameMovieLink) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNameMovieLink.getSchemaName(),
                    schemaTableNameMovieLink.getTableName(),
                    new ArrayList<>(List.of("movie_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNameTitle.getSchemaName(),
                    schemaTableNameTitle.getTableName(),
                    new ArrayList<>(List.of("movie_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        if (jdbcClient.getTableHandle(schemaTableNamePersonInfo) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNamePersonInfo.getSchemaName(),
                    schemaTableNamePersonInfo.getTableName(),
                    new ArrayList<>(List.of("info_type_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        if (jdbcClient.getTableHandle(schemaTableNameInfoType) == null) {
            jdbcClient.ingestRelation(
                    schemaTableNameInfoType.getSchemaName(),
                    schemaTableNameInfoType.getTableName(),
                    new ArrayList<>(List.of("info_type_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> base.createPhysicalPlanFromJoinOrdering(new JoinOrdering(List.of(schemaTableNameTitle, schemaTableNameMovieLink,
                        schemaTableNamePersonInfo, schemaTableNameInfoType))),
                "Expected to throw IllegalStateException"
        );
        assertTrue(thrown.getMessage().contains("allCandidateSchemaTableNames from [multiwaycomplex.testDPWhenThereIsCrossProduct_ml, multiwaycomplex.testDPWhenThereIsCrossProduct_it, multiwaycomplex.testDPWhenThereIsCrossProduct_pi, multiwaycomplex.testDPWhenThereIsCrossProduct_t] is empty, which shouldn't be"));
    }

    @Test
    public void testHJPhysicalHeuristics()
    {
        FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration =
                FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration.builder()
                        .enableHJPhysicalHeuristics(true)
                        .build();
        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new FindTheBestJoinOrderingWithDP(new PostgresCardEstProvider(true), ruleConfiguration)),
                ExecutionNormal.class,
                TestingMultiwayJoinDatabaseComplexSupplier.get().getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class))));

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

        Pair<Plan, List<Operator>> test = base.createPhysicalPlanFromJoinOrdering(new JoinOrdering(List.of(schemaTableNameRegion, schemaTableNameNation)));

        base.testPhysicalPlanExecution(Pair.of(test.getLeft(), test.getRight()));
        FindTheBestJoinOrderingWithDP.DPTable dpTable = test.getLeft().getPlanStatistics().getDpTable();
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(dpTable);
        }
        // check the top most DP cell
        FindTheBestJoinOrderingWithDP.DPTableEntry topMostEntry = dpTable.getBestPlan();
        assertEquals(List.of(schemaTableNameNation, schemaTableNameRegion), topMostEntry.getPlanAssociatedWithOptCost());
    }
}
