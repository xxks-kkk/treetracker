package org.zhu45.treetracker.relational.planner.cost;

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
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.rule.FindOptimalJoinTree;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.RuleStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoinTreeHeightProvider
{
    private static final Logger traceLogger = LogManager.getLogger(TestJoinTreeHeightProvider.class.getName());

    private static final String naturalJoinTable = "TestJoinTreeHeightProvider";

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestJoinTreeHeightProvider.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(JoinTreeHeightProvider.class.getName(), Level.DEBUG);
        }
    }

    public static Pair<Plan, List<Operator>> createJOB10a(TestingPhysicalPlanBase base)
    {
        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String prefix = "tJTHeight_";

        String castInfo = prefix + "cast_info";
        SchemaTableName schemaTableNameCastInfo = new SchemaTableName(schemaName, castInfo);
        if (jdbcClient.getTableHandle(schemaTableNameCastInfo) == null) {
            List<List<RelationalValue>> relationValCastInfo = new ArrayList<>(
                    List.of(List.of(IntegerValue.of(1), IntegerValue.of(1), IntegerValue.of(1), IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    castInfo,
                    new ArrayList<>(List.of("person_id", "movie_id", "person_role_id", "role_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER, INTEGER, INTEGER)),
                    relationValCastInfo);
        }

        String roleType = prefix + "role_type";
        SchemaTableName schemaTableNameRoleType = new SchemaTableName(schemaName, roleType);
        if (jdbcClient.getTableHandle(schemaTableNameRoleType) == null) {
            List<List<RelationalValue>> relationValRoleType = new ArrayList<>(
                    List.of(List.of(IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    roleType,
                    new ArrayList<>(List.of("role_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValRoleType);
        }

        String title = prefix + "title";
        SchemaTableName schemaTableNameTitle = new SchemaTableName(schemaName, title);
        if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
            List<List<RelationalValue>> relationValTitle = new ArrayList<>(
                    List.of(List.of(IntegerValue.of(1), IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    title,
                    new ArrayList<>(List.of("movie_id", "kind_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationValTitle);
        }

        String movieCompanies = prefix + "movie_companies";
        SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, movieCompanies);
        if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
            List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(
                    List.of(List.of(IntegerValue.of(1), IntegerValue.of(1), IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    movieCompanies,
                    new ArrayList<>(List.of("movie_id", "company_id", "company_type_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER, INTEGER)),
                    relationValMovieCompanies);
        }

        String companyName = prefix + "company_name";
        SchemaTableName schemaTableNameCompanyName = new SchemaTableName(schemaName, companyName);
        if (jdbcClient.getTableHandle(schemaTableNameCompanyName) == null) {
            List<List<RelationalValue>> relationValCompanyName = new ArrayList<>(
                    List.of(List.of(IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    companyName,
                    new ArrayList<>(List.of("company_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValCompanyName);
        }

        String companyType = prefix + "company_type";
        SchemaTableName schemaTableNameCompanyType = new SchemaTableName(schemaName, companyType);
        if (jdbcClient.getTableHandle(schemaTableNameCompanyType) == null) {
            List<List<RelationalValue>> relationValCompanyType = new ArrayList<>(
                    List.of(List.of(IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    companyType,
                    new ArrayList<>(List.of("company_type_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValCompanyType);
        }

        String charName = prefix + "char_name";
        SchemaTableName schemaTableNameCharName = new SchemaTableName(schemaName, charName);
        if (jdbcClient.getTableHandle(schemaTableNameCharName) == null) {
            List<List<RelationalValue>> relationValCharName = new ArrayList<>(
                    List.of(List.of(IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    charName,
                    new ArrayList<>(List.of("person_role_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationValCharName);
        }

        JoinOrdering joinOrdering = new JoinOrdering(List.of(schemaTableNameCastInfo, schemaTableNameRoleType,
                schemaTableNameTitle, schemaTableNameMovieCompanies, schemaTableNameCompanyName, schemaTableNameCharName,
                schemaTableNameCompanyType));
        return base.createPhysicalPlanFromJoinOrdering(joinOrdering);
    }

    @Test
    public void test()
            throws Exception
    {
        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();
        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new FindOptimalJoinTree(new JoinTreeHeightProvider())),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        Pair<Plan, List<Operator>> plan = createJOB10a(base);
        PlanStatistics planStatistics = plan.getKey().getPlanStatistics();
        RuleStatistics ruleStatistics = planStatistics.getRuleStatisticsList().get(0);
        Collection<Double> costs = ruleStatistics.getSearchedJoinTrees().values();
        assertEquals(2, costs.size());
        /**
         * imdb.q10a_cast_info(person_id,movie_id,person_role_id,role_id)
         * |imdb.q10a_role_type(role_id)
         * |imdb.q10a_title(movie_id,kind_id)
         * |imdb_int.movie_companies(movie_id,company_id,company_type_id)
         * ||imdb.q10a_company_name(company_id)
         * ||imdb_int.company_type(company_type_id)
         * |imdb_int.char_name(person_role_id)
         */
        assertTrue(costs.contains(3.0));
        /**
         * imdb.q10a_cast_info(person_id,movie_id,person_role_id,role_id)
         * |imdb.q10a_role_type(role_id)
         * |imdb.q10a_title(movie_id,kind_id)
         * ||imdb_int.movie_companies(movie_id,company_id,company_type_id)
         * |||imdb.q10a_company_name(company_id)
         * |||imdb_int.company_type(company_type_id)
         * |imdb_int.char_name(person_role_id)
         */
        assertTrue(costs.contains(4.0));
        base.tearDown();
    }
}
