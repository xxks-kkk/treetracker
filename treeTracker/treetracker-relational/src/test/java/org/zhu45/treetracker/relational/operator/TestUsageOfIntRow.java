package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBasedTreeTrackerTwoOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

/**
 * Test IntRow can be used for the expected query
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestUsageOfIntRow
{
    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestUsageOfIntRow";
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
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class))));

        printer = new StatisticsInformationPrinter();
    }

    @Test
    public void testUsageOfIntRow()
    {
        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();

        String movieCompanies = "movie_companies";
        SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, movieCompanies);
        if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
            List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new IntegerValue(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    movieCompanies,
                    new ArrayList<>(List.of("movie_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValMovieCompanies);
        }

        String movieInfo = "movie_info";
        SchemaTableName schemaTableNameMovieInfo = new SchemaTableName(schemaName, movieInfo);
        if (jdbcClient.getTableHandle(schemaTableNameMovieInfo) == null) {
            List<List<RelationalValue>> relationValMovieInfo = new ArrayList<>(Arrays.asList(
                    List.of(new IntegerValue(1),
                            new IntegerValue(2))));
            jdbcClient.ingestRelation(
                    schemaName,
                    movieInfo,
                    new ArrayList<>(Arrays.asList("movie_id", "info_type_id")),
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                    relationValMovieInfo);
        }

        String infoType = "info_type";
        SchemaTableName schemaTableNameInfoType = new SchemaTableName(schemaName, infoType);
        if (jdbcClient.getTableHandle(schemaTableNameInfoType) == null) {
            List<List<RelationalValue>> relationValInfoType = new ArrayList<>(Arrays.asList(
                    List.of(new IntegerValue(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    infoType,
                    new ArrayList<>(Arrays.asList("info_type_id")),
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relationValInfoType);
        }

        JoinOrdering joinOrdering = new JoinOrdering(List.of(schemaTableNameMovieCompanies, schemaTableNameMovieInfo, schemaTableNameInfoType));
        Pair<Plan, List<Operator>> plan = base.createPhysicalPlanFromJoinOrdering(joinOrdering);
        base.testPhysicalPlanExecution(plan);
        Operator rootOperator = plan.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertEquals(3, StringUtils.countMatches(statistics, "RecordTupleSourceClazzName: org.zhu45.treetracker.jdbc.RecordIntTupleSourceProvider"));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
