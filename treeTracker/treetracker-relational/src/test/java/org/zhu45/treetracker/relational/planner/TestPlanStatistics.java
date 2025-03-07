package org.zhu45.treetracker.relational.planner;

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
import org.zhu45.treetracker.jdbc.DriverConnectionFactory;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.DatabaseSuppler;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeTrueCostProvider;
import org.zhu45.treetracker.relational.planner.rule.DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee;
import org.zhu45.treetracker.relational.planner.rule.FindOptimalJoinTree;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPlanStatistics
{
    private static final String naturalJoinTable = "TestFindTheOptimalJoinTree";
    private TestingPhysicalPlanBase base;

    @BeforeAll
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
                List.of(new FindOptimalJoinTree(new JoinTreeTrueCostProvider()),
                        new DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee()),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
    }

    @Test
    public void testPlanStatistics()
    {
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
        // FIXME: FindOptimalJoinTree doesn't consider DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee during
        //  the search of the optimal join tree, which may need to fix (e.g., FindOptimalJoinTree can maintain a list of
        //  rules that can be considered during its application, i.e., compatible rules). This situation also implies
        //  an extra dimension to conisder (whether to use no-good list) when finding join ordering and join tree for TTJ
        assertEquals(1, plan.getKey().getPlanStatistics().getRulesApplied().size());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
