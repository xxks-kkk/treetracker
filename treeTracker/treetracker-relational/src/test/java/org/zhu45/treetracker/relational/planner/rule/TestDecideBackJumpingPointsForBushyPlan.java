package org.zhu45.treetracker.relational.planner.rule;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OperatorSpecification;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TestTupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanStatisticsInformation;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.plan.Side;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.OperatorSpecification.findTargetOperator;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.rule.DecideBackJumpingPointsForBushyPlan.CollectLeftDeepPlansContext.printLeftDeepPlans;
import static org.zhu45.treetracker.relational.planner.rule.DecideBackJumpingPointsForBushyPlan.collectLeftDeepPlansFromBushyPlan;
import static org.zhu45.treetracker.relational.planner.rule.DecideBackJumpingPointsForBushyPlan.obtainTempRelationName;
import static org.zhu45.treetracker.relational.planner.testing.postgresplan.PostgresPlanGenerator.generatePostgresPlanJson;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestDecideBackJumpingPointsForBushyPlan
{
    private static final Logger traceLogger;
    private static final String naturalJoinTable = "TestDecideBackJumpingPointsForBushyPlan";

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TestDecideBackJumpingPointsForBushyPlan.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestDecideBackJumpingPointsForBushyPlan.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(DecideBackJumpingPointsForBushyPlan.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class.getName(), Level.TRACE);
        }
    }

    public class TestDecideBackJumpingPointsForBushyPlanCases
            implements TestCases
    {
        private String schemaName;
        private JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestDecideBackJumpingPointsForBushyPlanCases(TestingPhysicalPlanBase base)
        {
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public void caseOne()
        {
            String relationT = "bushy_T";
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
            if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
                List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationT,
                        Arrays.asList("x", "y"),
                        Arrays.asList(INTEGER, INTEGER), relationValT);
            }

            String relationS = "bushy_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(0), IntegerValue.of(1)),
                        Arrays.asList(IntegerValue.of(0), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(0), IntegerValue.of(3)),
                        Arrays.asList(IntegerValue.of(0), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(0), IntegerValue.of(5))));
                jdbcClient.ingestRelation(schemaName, relationS,
                        Arrays.asList("y", "z"),
                        Arrays.asList(INTEGER, INTEGER), relationValS);
            }

            String relationH = "bushy_H";
            SchemaTableName schemaTableNameH = new SchemaTableName(schemaName, relationH);
            if (jdbcClient.getTableHandle(schemaTableNameH) == null) {
                List<List<RelationalValue>> relationValH = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationH,
                        Arrays.asList("z"),
                        Arrays.asList(INTEGER), relationValH);
            }

            String relationB = "bushy_B";
            SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(3), IntegerValue.of(0))));
                jdbcClient.ingestRelation(schemaName, relationB,
                        Arrays.asList("z", "w"),
                        Arrays.asList(INTEGER, INTEGER), relationValB);
            }

            String relationU = "bushy_U";
            SchemaTableName schemaTableNameU = new SchemaTableName(schemaName, relationU);
            if (jdbcClient.getTableHandle(schemaTableNameU) == null) {
                List<List<RelationalValue>> relationValU = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(0), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(3)),
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationU,
                        Arrays.asList("w", "t"),
                        Arrays.asList(INTEGER, INTEGER), relationValU);
            }

            String relationV = "bushy_V";
            SchemaTableName schemaTableNameV = new SchemaTableName(schemaName, relationV);
            if (jdbcClient.getTableHandle(schemaTableNameV) == null) {
                List<List<RelationalValue>> relationValV = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationV,
                        Arrays.asList("t"),
                        Arrays.asList(INTEGER), relationValV);
            }

            String relationR = "bushy_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationR,
                        Arrays.asList("z", "a"),
                        Arrays.asList(INTEGER, INTEGER), relationValR);
            }

            String relationG = "bushy_G";
            SchemaTableName schemaTableNameG = new SchemaTableName(schemaName, relationG);
            if (jdbcClient.getTableHandle(schemaTableNameG) == null) {
                List<List<RelationalValue>> relationValG = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationG,
                        Arrays.asList("a", "b"),
                        Arrays.asList(INTEGER, INTEGER), relationValG);
            }
        }

        public void caseTwo()
        {
            String relationMovieCompanies = "bushy_movie_companies";
            SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, relationMovieCompanies);
            if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
                List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(3))));
                jdbcClient.ingestRelation(schemaName, relationMovieCompanies,
                        Arrays.asList("movie_id", "company_id", "company_type_id"),
                        Arrays.asList(INTEGER, INTEGER, INTEGER), relationValMovieCompanies);
            }

            String relationCompanyType = "bushy_company_type";
            SchemaTableName schemaTableNameCompanyType = new SchemaTableName(schemaName, relationCompanyType);
            if (jdbcClient.getTableHandle(schemaTableNameCompanyType) == null) {
                List<List<RelationalValue>> relationValCompanyType = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationCompanyType,
                        Arrays.asList("company_type_id"),
                        Arrays.asList(INTEGER), relationValCompanyType);
            }

            String relationCompanyName = "bushy_company_name";
            SchemaTableName schemaTableNameCompanyName = new SchemaTableName(schemaName, relationCompanyName);
            if (jdbcClient.getTableHandle(schemaTableNameCompanyName) == null) {
                List<List<RelationalValue>> relationValCompanyName = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationCompanyName,
                        Arrays.asList("company_id"),
                        Arrays.asList(INTEGER), relationValCompanyName);
            }

            String relationCastInfo = "bushy_cast_info";
            SchemaTableName schemaTableNameCastInfo = new SchemaTableName(schemaName, relationCastInfo);
            if (jdbcClient.getTableHandle(schemaTableNameCastInfo) == null) {
                List<List<RelationalValue>> relationValCastInfo = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(2), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationCastInfo,
                        Arrays.asList("person_id", "movie_id", "person_role_id", "role_id"),
                        Arrays.asList(INTEGER, INTEGER, INTEGER, INTEGER), relationValCastInfo);
            }

            String relationTitle = "bushy_title";
            SchemaTableName schemaTableNameTitle = new SchemaTableName(schemaName, relationTitle);
            if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
                List<List<RelationalValue>> relationValTitle = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationTitle,
                        Arrays.asList("movie_id", "kind_id"),
                        Arrays.asList(INTEGER, INTEGER), relationValTitle);
            }

            String relationCharName = "bushy_char_name";
            SchemaTableName schemaTableNameCharName = new SchemaTableName(schemaName, relationCharName);
            if (jdbcClient.getTableHandle(schemaTableNameCharName) == null) {
                List<List<RelationalValue>> relationValCharName = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationCharName,
                        Arrays.asList("person_role_id"),
                        Arrays.asList(INTEGER), relationValCharName);
            }

            String relationRoleType = "bushy_role_type";
            SchemaTableName schemaTableNameRoleType = new SchemaTableName(schemaName, relationRoleType);
            if (jdbcClient.getTableHandle(schemaTableNameRoleType) == null) {
                List<List<RelationalValue>> relationValRoleType = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationRoleType,
                        Arrays.asList("role_id"),
                        Arrays.asList(INTEGER), relationValRoleType);
            }
        }

        public void caseThree()
        {
            String relationAkaName = "8b_aka_name";
            SchemaTableName schemaTableNameAkaName = new SchemaTableName(schemaName, relationAkaName);
            if (jdbcClient.getTableHandle(schemaTableNameAkaName) == null) {
                List<List<RelationalValue>> relationValAkaName = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationAkaName,
                        Arrays.asList("person_id"),
                        Arrays.asList(INTEGER), relationValAkaName);
            }

            String relationCastInfo = "8b_cast_info";
            SchemaTableName schemaTableNameCastInfo = new SchemaTableName(schemaName, relationCastInfo);
            if (jdbcClient.getTableHandle(schemaTableNameCastInfo) == null) {
                List<List<RelationalValue>> relationValCastInfo = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(2), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationCastInfo,
                        Arrays.asList("person_id", "movie_id", "person_role_id", "role_id"),
                        Arrays.asList(INTEGER, INTEGER, INTEGER, INTEGER), relationValCastInfo);
            }

            String relationCompanyName = "8b_company_name";
            SchemaTableName schemaTableNameCompanyName = new SchemaTableName(schemaName, relationCompanyName);
            if (jdbcClient.getTableHandle(schemaTableNameCompanyName) == null) {
                List<List<RelationalValue>> relationValCompanyName = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationCompanyName,
                        Arrays.asList("company_id"),
                        Arrays.asList(INTEGER), relationValCompanyName);
            }

            String relationMovieCompanies = "8b_movie_companies";
            SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, relationMovieCompanies);
            if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
                List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(3))));
                jdbcClient.ingestRelation(schemaName, relationMovieCompanies,
                        Arrays.asList("movie_id", "company_id", "company_type_id"),
                        Arrays.asList(INTEGER, INTEGER, INTEGER), relationValMovieCompanies);
            }

            String relationName = "8b_name";
            SchemaTableName schemaTableNameName = new SchemaTableName(schemaName, relationName);
            if (jdbcClient.getTableHandle(schemaTableNameName) == null) {
                List<List<RelationalValue>> relationValName = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationName,
                        Arrays.asList("role_id"),
                        Arrays.asList(INTEGER), relationValName);
            }

            String relationRoleType = "8b_role_type";
            SchemaTableName schemaTableNameRoleType = new SchemaTableName(schemaName, relationRoleType);
            if (jdbcClient.getTableHandle(schemaTableNameRoleType) == null) {
                List<List<RelationalValue>> relationValRoleType = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationRoleType,
                        Arrays.asList("role_id"),
                        Arrays.asList(INTEGER), relationValRoleType);
            }

            String relationTitle = "8b_title";
            SchemaTableName schemaTableNameTitle = new SchemaTableName(schemaName, relationTitle);
            if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
                List<List<RelationalValue>> relationValTitle = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationTitle,
                        Arrays.asList("movie_id", "kind_id"),
                        Arrays.asList(INTEGER, INTEGER), relationValTitle);
            }
        }

        public void caseFour()
        {
            String relationAkaTitle = "15a_aka_title";
            SchemaTableName schemaTableNameAkaTitle = new SchemaTableName(schemaName, relationAkaTitle);
            if (jdbcClient.getTableHandle(schemaTableNameAkaTitle) == null) {
                List<List<RelationalValue>> relationValAkaTitle = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationAkaTitle,
                        Arrays.asList("movie_id"),
                        Arrays.asList(INTEGER), relationValAkaTitle);
            }

            String relationCompanyName = "15a_company_name";
            SchemaTableName schemaTableNameCompanyName = new SchemaTableName(schemaName, relationCompanyName);
            if (jdbcClient.getTableHandle(schemaTableNameCompanyName) == null) {
                List<List<RelationalValue>> relationValCompanyName = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationCompanyName,
                        Arrays.asList("company_id"),
                        Arrays.asList(INTEGER), relationValCompanyName);
            }

            String relationCompanyType = "15a_company_type";
            SchemaTableName schemaTableNameCompanyType = new SchemaTableName(schemaName, relationCompanyType);
            if (jdbcClient.getTableHandle(schemaTableNameCompanyType) == null) {
                List<List<RelationalValue>> relationValCompanyType = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationCompanyType,
                        Arrays.asList("company_type_id"),
                        Arrays.asList(INTEGER), relationValCompanyType);
            }

            String relationInfoType = "15a_info_type";
            SchemaTableName schemaTableNameInfoType = new SchemaTableName(schemaName, relationInfoType);
            if (jdbcClient.getTableHandle(schemaTableNameInfoType) == null) {
                List<List<RelationalValue>> relationValInfoType = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationInfoType,
                        Arrays.asList("info_type_id"),
                        Arrays.asList(INTEGER), relationValInfoType);
            }

            String relationKeyword = "15a_keyword";
            SchemaTableName schemaTableNameKeyword = new SchemaTableName(schemaName, relationKeyword);
            if (jdbcClient.getTableHandle(schemaTableNameKeyword) == null) {
                List<List<RelationalValue>> relationValKeyword = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationKeyword,
                        Arrays.asList("keyword_id"),
                        Arrays.asList(INTEGER), relationValKeyword);
            }

            String relationMovieCompanies = "15a_movie_companies";
            SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, relationMovieCompanies);
            if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
                List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(3))));
                jdbcClient.ingestRelation(schemaName, relationMovieCompanies,
                        Arrays.asList("movie_id", "company_id", "company_type_id"),
                        Arrays.asList(INTEGER, INTEGER, INTEGER), relationValMovieCompanies);
            }

            String relationMovieInfo = "15a_movie_info";
            SchemaTableName schemaTableNameMovieInfo = new SchemaTableName(schemaName, relationMovieInfo);
            if (jdbcClient.getTableHandle(schemaTableNameMovieInfo) == null) {
                List<List<RelationalValue>> relationValMovieInfo = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationMovieInfo,
                        Arrays.asList("movie_id", "info_type_id"),
                        Arrays.asList(INTEGER, INTEGER), relationValMovieInfo);
            }

            String relationMovieKeyword = "15a_movie_keyword";
            SchemaTableName schemaTableNameMovieKeyword = new SchemaTableName(schemaName, relationMovieKeyword);
            if (jdbcClient.getTableHandle(schemaTableNameMovieKeyword) == null) {
                List<List<RelationalValue>> relationValMovieKeyword = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationMovieKeyword,
                        Arrays.asList("movie_id", "keyword_id"),
                        Arrays.asList(INTEGER, INTEGER), relationValMovieKeyword);
            }

            String relationTitle = "15a_title";
            SchemaTableName schemaTableNameTitle = new SchemaTableName(schemaName, relationTitle);
            if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
                List<List<RelationalValue>> relationValTitle = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationTitle,
                        Arrays.asList("movie_id", "kind_id"),
                        Arrays.asList(INTEGER, INTEGER), relationValTitle);
            }
        }

        public void caseFive()
        {
            String relationLineItem = "8W_lineitem";
            SchemaTableName schemaTableNameLineItem = new SchemaTableName(schemaName, relationLineItem);
            if (jdbcClient.getTableHandle(schemaTableNameLineItem) == null) {
                List<List<RelationalValue>> relationValLineItem = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2), IntegerValue.of(2), IntegerValue.of(3))));
                jdbcClient.ingestRelation(schemaName, relationLineItem,
                        Arrays.asList("orderkey", "partkey", "suppkey"),
                        Arrays.asList(INTEGER, INTEGER, INTEGER), relationValLineItem);
            }

            String relationPart = "8W_part";
            SchemaTableName schemaTableNamePart = new SchemaTableName(schemaName, relationPart);
            if (jdbcClient.getTableHandle(schemaTableNamePart) == null) {
                List<List<RelationalValue>> relationValPart = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationPart,
                        Arrays.asList("partkey"),
                        Arrays.asList(INTEGER), relationValPart);
            }

            String relationOrders = "8W_orders";
            SchemaTableName schemaTableNameOrders = new SchemaTableName(schemaName, relationOrders);
            if (jdbcClient.getTableHandle(schemaTableNameOrders) == null) {
                List<List<RelationalValue>> relationValOrders = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationOrders,
                        Arrays.asList("orderkey", "custkey"),
                        Arrays.asList(INTEGER, INTEGER), relationValOrders);
            }

            String relationCustomer = "8W_customer";
            SchemaTableName schemaTableNameCustomer = new SchemaTableName(schemaName, relationCustomer);
            if (jdbcClient.getTableHandle(schemaTableNameCustomer) == null) {
                List<List<RelationalValue>> relationValCustomer = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(2), IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationCustomer,
                        Arrays.asList("custkey", "nationkey"),
                        Arrays.asList(INTEGER, INTEGER), relationValCustomer);
            }

            String relationNation = "8W_nation";
            SchemaTableName schemaTableNameNation = new SchemaTableName(schemaName, relationNation);
            if (jdbcClient.getTableHandle(schemaTableNameNation) == null) {
                List<List<RelationalValue>> relationValNation = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationNation,
                        Arrays.asList("nationkey", "regionkey"),
                        Arrays.asList(INTEGER, INTEGER), relationValNation);
            }

            String relationRegion = "8W_region";
            SchemaTableName schemaTableNameRegion = new SchemaTableName(schemaName, relationRegion);
            if (jdbcClient.getTableHandle(schemaTableNameRegion) == null) {
                List<List<RelationalValue>> relationValRegion = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(3))));
                jdbcClient.ingestRelation(schemaName, relationRegion,
                        Arrays.asList("regionkey"),
                        Arrays.asList(INTEGER), relationValRegion);
            }

            String relationSupplier = "8W_supplier";
            SchemaTableName schemaTableNameSupplier = new SchemaTableName(schemaName, relationSupplier);
            if (jdbcClient.getTableHandle(schemaTableNameSupplier) == null) {
                List<List<RelationalValue>> relationValSupplier = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationSupplier,
                        Arrays.asList("suppkey", "nationkey2"),
                        Arrays.asList(INTEGER, INTEGER), relationValSupplier);
            }

            String relationNation2 = "8W_nation2";
            SchemaTableName schemaTableNameNation2 = new SchemaTableName(schemaName, relationNation2);
            if (jdbcClient.getTableHandle(schemaTableNameNation2) == null) {
                List<List<RelationalValue>> relationValNation2 = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationNation2,
                        Arrays.asList("nationkey2", "regionkey2"),
                        Arrays.asList(INTEGER, INTEGER), relationValNation2);
            }
        }
    }

    @Test
    public void testCollectLeftDeepPlansFromBushyPlan()
    {
        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                TestingMultiwayJoinDatabaseComplexSupplier.get(),
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class))));
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                "|JOIN,Inner\n" +
                "||TAB,caseone_T,Outer\n" +
                "||TAB,caseone_S,Inner\n" +
                "|JOIN,Outer\n" +
                "||TAB,caseone_B,Inner\n" +
                "||TAB,caseone_R,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());
        DecideBackJumpingPointsForBushyPlan.CollectLeftDeepPlansResult results = collectLeftDeepPlansFromBushyPlan(pair.getLeft().getRoot());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            System.out.println(printLeftDeepPlans(results.leftDeepPlans));
        }
        assertEquals(2, results.leftDeepPlans.size());
        assertTrue(results.nodeToSchemaTableNames.containsValue(List.of(new SchemaTableName(base.getDatabase().getSchemaName(), "caseone_R"),
                new SchemaTableName(base.getDatabase().getSchemaName(), "caseone_B"),
                new SchemaTableName(base.getDatabase().getSchemaName(), "caseone_T"))));
        assertTrue(results.nodeToSchemaTableNames.containsValue(List.of(new SchemaTableName(base.getDatabase().getSchemaName(), "caseone_T"),
                new SchemaTableName(base.getDatabase().getSchemaName(), "caseone_S"))));
    }

    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void test1(TestingPhysicalPlanBase base)
    {
        TestDecideBackJumpingPointsForBushyPlanCases cases = new TestDecideBackJumpingPointsForBushyPlanCases(base);
        cases.caseOne();
        String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                "|JOIN,Inner\n" +
                "||TAB,bushy_R,Outer\n" +
                "||TAB,bushy_G,Inner\n" +
                "|JOIN,Outer\n" +
                "||JOIN,Inner\n" +
                "|||TAB,bushy_B,Outer\n" +
                "|||JOIN,Inner\n" +
                "||||TAB,bushy_U,Outer\n" +
                "||||TAB,bushy_V,Inner\n" +
                "||JOIN,Outer\n" +
                "|||JOIN,Inner\n" +
                "||||TAB,bushy_S,Outer\n" +
                "||||TAB,bushy_H,Inner\n" +
                "|||TAB,bushy_T,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            System.out.println(pair.getKey().getPlanStatistics().getOptimalJoinTree());
        }
        assertEquals(
                List.of("bushy_T", "bushy_S", "bushy_H", "bushy_B", "bushy_U",
                        "bushy_V", "bushy_R", "bushy_G"),
                pair.getKey().getPlanStatistics().getOptimalJoinTree().getTraversalList().stream()
                        .map(MultiwayJoinNode::getSchemaTableName)
                        .map(SchemaTableName::getTableName)
                        .collect(Collectors.toList()));
        assertEquals("multiwaycomplex.bushy_T(x,y)\n" +
                "|multiwaycomplex.bushy_S(y,z)\n" +
                "||multiwaycomplex.bushy_B(w,z)\n" +
                "|||multiwaycomplex.bushy_U(t,w)\n" +
                "||||multiwaycomplex.bushy_V(t)\n" +
                "||multiwaycomplex.bushy_R(a,z)\n" +
                "|||multiwaycomplex.bushy_G(a,b)\n" +
                "||multiwaycomplex.bushy_H(z)", pair.getKey().getPlanStatistics().getOptimalJoinTree().toString());
        List<String> relationNamesWithTTJScan = List.of("bushy_T", "bushy_S", "bushy_B", "bushy_U", "bushy_R");
        checkTTJScanSet(pair.getValue(), relationNamesWithTTJScan);
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            PlanPrinter printer = new PlanPrinter(pair.getKey().getRoot());
            System.out.println(printer.toText(0));
        }
        {
            OperatorSpecification leftOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_U")
                    .build();
            OperatorSpecification rightOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_V")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(leftOperator, rightOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals("bushy_U", targetOperator.getMultiwayJoinNode().getSchemaTableName().getTableName());
        }
        {
            OperatorSpecification leftOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_S")
                    .build();
            OperatorSpecification rightOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_H")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(leftOperator, rightOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals("bushy_S", targetOperator.getMultiwayJoinNode().getSchemaTableName().getTableName());
        }
        {
            OperatorSpecification leftOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_B")
                    .build();
            OperatorSpecification rightOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .relationName("bushy_U")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(leftOperator, rightOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals("bushy_B", targetOperator.getMultiwayJoinNode().getSchemaTableName().getTableName());
        }
        {
            OperatorSpecification leftOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_R")
                    .build();
            OperatorSpecification rightOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_G")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(leftOperator, rightOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals("bushy_R", targetOperator.getMultiwayJoinNode().getSchemaTableName().getTableName());
        }
        base.testPhysicalPlanExecution(pair);
    }

    /**
     * Mainly check that when a join operator that has Side.INNER and itself is the root of a left-deep plan,
     * the join operator has MultiwayJoinNode bound to the left-most relation of the left-deep subplan.
     */
    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void test2(TestingPhysicalPlanBase base)
    {
        TestDecideBackJumpingPointsForBushyPlanCases cases = new TestDecideBackJumpingPointsForBushyPlanCases(base);
        cases.caseOne();
        String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                "|JOIN,Inner\n" +
                "||JOIN,Outer\n" +
                "|||TAB,bushy_B,Outer\n" +
                "|||TAB,bushy_U,Inner\n" +
                "||TAB,bushy_V,Inner\n" +
                "|TAB,bushy_S,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            System.out.println(pair.getKey().getPlanStatistics().getOptimalJoinTree());
        }
        assertEquals(
                List.of("bushy_S", "bushy_B", "bushy_U", "bushy_V"),
                pair.getKey().getPlanStatistics().getOptimalJoinTree().getTraversalList().stream()
                        .map(MultiwayJoinNode::getSchemaTableName)
                        .map(SchemaTableName::getTableName)
                        .collect(Collectors.toList()));
        assertEquals("multiwaycomplex.bushy_S(y,z)\n" +
                "|multiwaycomplex.bushy_B(w,z)\n" +
                "||multiwaycomplex.bushy_U(t,w)\n" +
                "|||multiwaycomplex.bushy_V(t)", pair.getKey().getPlanStatistics().getOptimalJoinTree().toString());
        List<String> relationNamesWithTTJScan = List.of("bushy_S", "bushy_B");
        checkTTJScanSet(pair.getValue(), relationNamesWithTTJScan);
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            PlanPrinter printer = new PlanPrinter(pair.getKey().getRoot());
            System.out.println(printer.toText(0));
        }
        OperatorSpecification leftOperator = OperatorSpecification.builder()
                .optType(OptType.join)
                .build();
        OperatorSpecification rightOperator = OperatorSpecification.builder()
                .optType(OptType.table)
                .relationName("bushy_V")
                .build();
        OperatorSpecification rootOperator = OperatorSpecification.builder()
                .optType(OptType.join)
                .children(List.of(leftOperator, rightOperator))
                .build();
        Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
        assertEquals("bushy_B", targetOperator.getMultiwayJoinNode().getSchemaTableName().getTableName());
        base.testPhysicalPlanExecution(pair);
    }

    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void test3(TestingPhysicalPlanBase base)
    {
        TestTupleBasedHashJoinOperator.TestBushyPlanTestCases cases = new TestTupleBasedHashJoinOperator.TestBushyPlanTestCases(base);
        var pair = cases.test7();
        assertEquals(
                List.of("caseseven_title", "caseseven_movie_info_idx", "caseseven_movie_companies", "caseseven_company_type", "caseseven_info_type"),
                pair.getKey().getPlanStatistics().getOptimalJoinTree().getTraversalList().stream()
                        .map(MultiwayJoinNode::getSchemaTableName)
                        .map(SchemaTableName::getTableName)
                        .collect(Collectors.toList()));
        assertEquals("multiwaycomplex.caseseven_title(movie_id,kind_id)\n" +
                "|multiwaycomplex.caseseven_movie_info_idx(info_type_id,movie_id)\n" +
                "||multiwaycomplex.caseseven_movie_companies(company_id,company_type_id,movie_id)\n" +
                "|||multiwaycomplex.caseseven_company_type(company_type_id)\n" +
                "||multiwaycomplex.caseseven_info_type(info_type_id)", pair.getKey().getPlanStatistics().getOptimalJoinTree().toString());
        List<String> relationNamesWithTTJScan = List.of("caseseven_title", "caseseven_movie_info_idx", "caseseven_movie_companies");
        checkTTJScanSet(pair.getValue(), relationNamesWithTTJScan);
        {
            OperatorSpecification joinOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .build();
            OperatorSpecification tableOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("caseseven_info_type")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .relationName("caseseven_movie_info_idx")
                    .children(List.of(joinOperator, tableOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals("caseseven_movie_info_idx", targetOperator.getSchemaTableName().getTableName());
            assertEquals(Side.INNER, targetOperator.getSide());
        }
        {
            OperatorSpecification leftOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("caseseven_movie_info_idx")
                    .build();
            OperatorSpecification tableOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .relationName("caseseven_movie_companies")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(leftOperator, tableOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals(Side.OUTER, targetOperator.getSide());
        }
        {
            OperatorSpecification leftOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("caseseven_movie_companies")
                    .build();
            OperatorSpecification rightOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("caseseven_company_type")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(leftOperator, rightOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals(Side.INNER, targetOperator.getSide());
        }
        base.testPhysicalPlanExecution(pair);
    }

    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void test4(TestingPhysicalPlanBase base)
    {
        TestDecideBackJumpingPointsForBushyPlanCases cases = new TestDecideBackJumpingPointsForBushyPlanCases(base);
        cases.caseOne();
        String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                "|JOIN,Inner\n" +
                "||JOIN,Outer\n" +
                "|||TAB,bushy_U,Outer\n" +
                "|||TAB,bushy_V,Inner\n" +
                "||TAB,bushy_B,Inner\n" +
                "|TAB,bushy_S,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());

        var node2JoinTrees = pair.getLeft().getPlanStatistics().getRuleStatisticsList().get(0).getNode2JoinTrees();
        assertEquals(2, node2JoinTrees.size());
        List<MultiwayJoinOrderedGraph> joinTrees = new ArrayList<>(node2JoinTrees.values());
        assertEquals("multiwaycomplex.bushy_U(w,t)\n" +
                "|multiwaycomplex.bushy_V(t)\n" +
                "|multiwaycomplex.bushy_B(w,z)", joinTrees.get(0).toString());
        assertEquals("multiwaycomplex.bushy_S(y,z)\n" +
                "|multiwaycomplex.temp_bushy_U(t,w,z)", joinTrees.get(1).toString());

        List<String> relationNamesWithTTJScan = List.of("bushy_S", "bushy_U");
        checkTTJScanSet(pair.getValue(), relationNamesWithTTJScan);

        {
            OperatorSpecification leftOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .build();
            OperatorSpecification rightOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_B")
                    .build();
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.join)
                    .relationName("bushy_U")
                    .virtualRelationName(obtainTempRelationName("bushy_U"))
                    .children(List.of(leftOperator, rightOperator))
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            assertEquals(Side.INNER, targetOperator.getSide());
        }

        base.testPhysicalPlanExecution(pair);

        {
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_U")
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            TupleBasedTableScanStatisticsInformation noGoodListMapStatisticsInformation = (TupleBasedTableScanStatisticsInformation) targetOperator.getNoGoodListMap()
                    .getStatisticsInformation();
            assertEquals(2, noGoodListMapStatisticsInformation.getNumberOfNoGoodTuples());
        }

        {
            OperatorSpecification rootOperator = OperatorSpecification.builder()
                    .optType(OptType.table)
                    .relationName("bushy_S")
                    .build();
            Operator targetOperator = requireNonNull(findTargetOperator(pair.getLeft().getRoot(), rootOperator));
            TupleBasedTableScanStatisticsInformation noGoodListMapStatisticsInformation = (TupleBasedTableScanStatisticsInformation) targetOperator.getNoGoodListMap()
                    .getStatisticsInformation();
            assertEquals(4, noGoodListMapStatisticsInformation.getNumberOfNoGoodTuples());
        }
    }

    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void testQ10a(TestingPhysicalPlanBase base)
    {
        TestDecideBackJumpingPointsForBushyPlanCases cases = new TestDecideBackJumpingPointsForBushyPlanCases(base);
        cases.caseTwo();
        String postgresPlan = generatePostgresPlanJson("AGG,Outer\n" +
                "|JOIN,Outer\n" +
                "||HASH,Inner\n" +
                "|||TAB,bushy_company_type,Outer\n" +
                "||GATHER,Outer\n" +
                "|||JOIN,Outer\n" +
                "||||TAB,bushy_char_name,Outer\n" +
                "||||HASH,Inner\n" +
                "|||||JOIN,Inner\n" +
                "||||||TAB,bushy_title,Outer\n" +
                "||||||HASH,Inner\n" +
                "|||||||JOIN,Outer\n" +
                "||||||||JOIN,Outer\n" +
                "|||||||||TAB,bushy_cast_info,Outer\n" +
                "|||||||||HASH,Inner\n" +
                "||||||||||TAB,bushy_role_type,Outer\n" +
                "||||||||HASH,Inner\n" +
                "|||||||||JOIN,Outer\n" +
                "||||||||||TAB,bushy_movie_companies,Outer\n" +
                "||||||||||HASH,Inner\n" +
                "|||||||||||TAB,bushy_company_name,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            System.out.println(pair.getKey().getPlanStatistics().getOptimalJoinTree());
        }
        base.testPhysicalPlanExecution(pair);
    }

    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void testQ8b(TestingPhysicalPlanBase base)
    {
        TestDecideBackJumpingPointsForBushyPlanCases cases = new TestDecideBackJumpingPointsForBushyPlanCases(base);
        cases.caseThree();
        String postgresPlan = generatePostgresPlanJson("AGG,Outer\n" +
                "|GATHER,Outer\n" +
                "||AGG,Outer\n" +
                "|||JOIN,Outer\n" +
                "||||HASH,Inner\n" +
                "|||||TAB,8b_role_type,Outer\n" +
                "||||JOIN,Outer\n" +
                "|||||TAB,8b_name,Outer\n" +
                "|||||HASH,Inner\n" +
                "||||||JOIN,Outer\n" +
                "|||||||TAB,8b_company_name,Outer\n" +
                "|||||||HASH,Inner\n" +
                "||||||||JOIN,Outer\n" +
                "|||||||||TAB,8b_movie_companies,Outer\n" +
                "|||||||||HASH,Inner\n" +
                "||||||||||JOIN,Outer\n" +
                "|||||||||||TAB,8b_aka_name,Outer\n" +
                "|||||||||||HASH,Inner\n" +
                "||||||||||||JOIN,Outer\n" +
                "|||||||||||||TAB,8b_cast_info,Outer\n" +
                "|||||||||||||HASH,Inner\n" +
                "||||||||||||||TAB,8b_title,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());

        base.testPhysicalPlanExecution(pair);
    }

    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void testQ15a(TestingPhysicalPlanBase base)
    {
        TestDecideBackJumpingPointsForBushyPlanCases cases = new TestDecideBackJumpingPointsForBushyPlanCases(base);
        cases.caseFour();
        String postgresPlan = generatePostgresPlanJson("AGG,Outer\n" +
                "|GATHER,Outer\n" +
                "||AGG,Outer\n" +
                "|||JOIN,Outer\n" +
                "||||HASH,Inner\n" +
                "|||||TAB,15a_keyword,Outer\n" +
                "||||JOIN,Outer\n" +
                "|||||TAB,15a_movie_keyword,Outer\n" +
                "|||||HASH,Inner\n" +
                "||||||JOIN,Outer\n" +
                "|||||||TAB,15a_title,Outer\n" +
                "|||||||HASH,Inner\n" +
                "||||||||JOIN,Outer\n" +
                "|||||||||HASH,Inner\n" +
                "||||||||||TAB,15a_company_type,Outer\n" +
                "|||||||||JOIN,Outer\n" +
                "||||||||||TAB,15a_company_name,Outer\n" +
                "||||||||||HASH,Inner\n" +
                "|||||||||||JOIN,Outer\n" +
                "||||||||||||TAB,15a_movie_companies,Outer\n" +
                "||||||||||||HASH,Inner\n" +
                "|||||||||||||JOIN,Outer\n" +
                "||||||||||||||TAB,15a_aka_title,Outer\n" +
                "||||||||||||||HASH,Inner\n" +
                "|||||||||||||||JOIN,Outer\n" +
                "||||||||||||||||TAB,15a_movie_info,Outer\n" +
                "||||||||||||||||HASH,Inner\n" +
                "|||||||||||||||||TAB,15a_info_type,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());

        base.testPhysicalPlanExecution(pair);
    }

    @ParameterizedTest
    @MethodSource("testDecideBackJumpingPointsForBushyPlanDataProvider")
    public void testQ8W(TestingPhysicalPlanBase base)
    {
        TestDecideBackJumpingPointsForBushyPlanCases cases = new TestDecideBackJumpingPointsForBushyPlanCases(base);
        cases.caseFive();
        String postgresPlan = generatePostgresPlanJson("AGG,Outer\n" +
                "|GATHER,Outer\n" +
                "||AGG,Outer\n" +
                "|||JOIN,Outer\n" +
                "||||HASH,Inner\n" +
                "|||||TAB,8W_nation2,Outer\n" +
                "||||JOIN,Outer\n" +
                "|||||HASH,Inner\n" +
                "||||||TAB,8W_supplier,Outer\n" +
                "|||||JOIN,Outer\n" +
                "||||||HASH,Inner\n" +
                "|||||||JOIN,Outer\n" +
                "||||||||TAB,8W_nation,Outer\n" +
                "||||||||HASH,Inner\n" +
                "|||||||||TAB,8W_region,Outer\n" +
                "||||||JOIN,Outer\n" +
                "|||||||TAB,8W_customer,Outer\n" +
                "|||||||HASH,Inner\n" +
                "||||||||JOIN,Outer\n" +
                "|||||||||TAB,8W_orders,Outer\n" +
                "|||||||||HASH,Inner\n" +
                "||||||||||JOIN,Outer\n" +
                "|||||||||||TAB,8W_lineitem,Outer\n" +
                "|||||||||||HASH,Inner\n" +
                "||||||||||||TAB,8W_part,Outer");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());

        base.testPhysicalPlanExecution(pair);
    }

    private static void checkTTJScanSet(List<Operator> operators,
                                        List<String> relationNamesWithTTJScan)
    {
        for (Operator operator : operators) {
            if (operator.getOperatorType() == OptType.table) {
                if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                    System.out.printf("%s has class: %s%n", operator.getSchemaTableName(), operator.getClass().getSimpleName());
                }
                if (operator.getClass().getSimpleName().equals("TupleBasedHighPerfTableScanOperator")) {
                    assertTrue(relationNamesWithTTJScan.contains(operator.getSchemaTableName().getTableName()),
                            operator.getSchemaTableName().getTableName() + " has unexpected TTJ Scan");
                }
                else {
                    assertFalse(relationNamesWithTTJScan.contains(operator.getSchemaTableName().getTableName()),
                            operator.getSchemaTableName().getTableName() + " should have TTJ Scan");
                }
            }
        }
    }

    private Stream<Arguments> testDecideBackJumpingPointsForBushyPlanDataProvider()
    {
        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                TestingMultiwayJoinDatabaseComplexSupplier.get(),
                naturalJoinTable,
                List.of(new DecideBackJumpingPointsForBushyPlan()),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        TestingPhysicalPlanBase base2 = new TestingPhysicalPlanBase(
                TestingMultiwayJoinDatabaseComplexSupplier.get(),
                naturalJoinTable,
                List.of(new DecideBackJumpingPointsForBushyPlan()),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class))));
        return Stream.of(Arguments.of(base),
                Arguments.of(base2));
    }
}
