package org.zhu45.treetracker.relational.planner;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.jdbc.DriverConnectionFactory;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.DatabaseSuppler;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeTrueCostProvider;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.rule.FindOptimalJoinTree;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFindTheOptimalJoinTree
{
    private static final String naturalJoinTable = "TestFindTheOptimalJoinTree";
    private TestingPhysicalPlanBase base;

    @BeforeEach
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(DatabaseSuppler.class.getName(), Level.INFO);
            Configurator.setAllLevels(TestingMultiwayJoinDatabaseComplex.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(FindOptimalJoinTree.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(DriverConnectionFactory.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new FindOptimalJoinTree(new JoinTreeTrueCostProvider())),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
    }

    /**
     * This test the rule is able to construct all possible join trees and find the optimal join tree.
     * Then, the resulting plan uses that join tree and can be evaluated correctly.
     */
    @Test
    public void testFindOptimalJoinTree() {
        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();

        String relationT = "casefour_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue(VARCHAR, "green")),
                    Collections.singletonList(new StringValue(VARCHAR, "green")),
                    Collections.singletonList(new StringValue(VARCHAR, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationT,
                    new ArrayList<>(List.of("x")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationValT);
        }

        String relationS = "casefour_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                    List.of(new StringValue(VARCHAR, "red"),
                            new StringValue(VARCHAR, "1"),
                            new StringValue(VARCHAR, "2")),
                    Arrays.asList(new StringValue(VARCHAR, "red"),
                            new StringValue(VARCHAR, "3"),
                            new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationS,
                    new ArrayList<>(Arrays.asList("x", "y", "z")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                    relationValS);
        }

        String relationB = "casefour_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(List.of(
                    Collections.singletonList(new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationB,
                    new ArrayList<>(List.of("z")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationValB);
        }

        String relationR = "casefour_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(
                    Arrays.asList(new StringValue(VARCHAR, "3"),
                            new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationR,
                    new ArrayList<>(Arrays.asList("y", "z")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                    relationValR);
        }
        JoinOrdering joinOrdering = new JoinOrdering(List.of(schemaTableNameS, schemaTableNameR, schemaTableNameB, schemaTableNameT));
        Pair<Plan, List<Operator>> plan = base.createPhysicalPlanFromJoinOrdering(joinOrdering);
        base.testPhysicalPlanExecution(plan);
        // Two possible join trees for the given ordering: (S (R (B)) (T)) and (S (R) (B) (T))
        assertEquals(2, plan.getKey().getPlanStatistics().getSearchedJoinTrees().size());
        save(plan.getKey().getPlanStatistics());
    }

    /**
     * In this test, we check if the rule handles the unique attribute case.
     */
    @Test
    public void testFindOptimalJoinTree2() {
        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();

        String movie_companies = "movie_companies";
        SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, movie_companies);
        if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
            List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new IntegerValue(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    movie_companies,
                    new ArrayList<>(List.of("movie_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValMovieCompanies);
        }

        String title = "title";
        SchemaTableName schemaTableNameTitle = new SchemaTableName(schemaName, title);
        if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
            List<List<RelationalValue>> relationValTitle = new ArrayList<>(Arrays.asList(
                    List.of(new IntegerValue(1),
                            new IntegerValue(2))));
            jdbcClient.ingestRelation(
                    schemaName,
                    title,
                    new ArrayList<>(Arrays.asList("movie_id", "kind_id")),
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                    relationValTitle);
        }

        JoinOrdering joinOrdering = new JoinOrdering(List.of(schemaTableNameMovieCompanies, schemaTableNameTitle));
        Pair<Plan, List<Operator>> plan = base.createPhysicalPlanFromJoinOrdering(joinOrdering);
        base.testPhysicalPlanExecution(plan);
    }

    /**
     * In this test, we check if the rule handles the unique attribute case. Specific, unique attribute has to with respect to the relations
     * have been processed not including attributes with respect to relations that are yet to be processed. In this example, both movie_info
     * and info_type have info_type_id but when process movie_info to generate join tree, info_type_id is an unique attribute because info_type
     * is yet to be processed.
     */
    @Test
    public void testFindOptimalJoinTree3() {
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
    }

    private void save(PlanStatistics planStatistics)
    {
        String queryName = "dummy";
        Path path = Paths.get("src", "test", "resources", "unitTestDummyResult");
        File file = path.toFile();
        file.mkdirs();
        Path savedPath = planStatistics.save(queryName, JoinOperator.TTJHP, path.toString());
        try {
            try (Stream<String> stream = Files.lines(savedPath)) {
                assertTrue(stream.count() > 1);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
