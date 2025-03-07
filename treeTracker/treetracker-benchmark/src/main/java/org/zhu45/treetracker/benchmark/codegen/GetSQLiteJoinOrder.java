package org.zhu45.treetracker.benchmark.codegen;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.job.IMDBDatabase;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.benchmark.ssb.SSBDatabase;
import org.zhu45.treetracker.benchmark.ssb.SSBQueries;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcSupplier;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.ROOT;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_SQLITE_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.akaName;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.akaTitle;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.castInfo;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.charName;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.compCastType;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.companyName;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.companyType;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.completeCast;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaTitleInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompCastTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompleteCastInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getLinkTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieLinkInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getPersonInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.infoType;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.keyword;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.kindType;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.linkType;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.movieCompanies;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.movieInfo;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.movieInfoIdx;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.movieKeyword;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.movieLink;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.name;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.personInfo;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.roleType;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.title;
import static org.zhu45.treetracker.benchmark.job.VerifySQLiteQueryIntegrity.constructQueries;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.customer;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.date;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getCustomerInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getDateInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getLineOrderInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getPartInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getSupplierInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.lineorder;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.part;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.supplier;

/**
 * We aim to obtain join order from SQLite. The necessary steps are:
 * 1. extract join order from SQLite
 * 2. create planStatistics.json
 */
public class GetSQLiteJoinOrder
{
    private static final String thirdPartyPath;
    public static final String jobQueriesPath;
    private static final String tpchQueriesPath;
    private static final String ssbQueriesPath;

    static {
        thirdPartyPath = Paths.get(ROOT, "third-party").toString();
        jobQueriesPath = Paths.get(thirdPartyPath, "join-order-benchmark").toString();
        tpchQueriesPath = Paths.get(thirdPartyPath, "tpc-h").toString();
        ssbQueriesPath = Paths.get(thirdPartyPath, "star-schema-benchmark").toString();
    }

    @Getter
    @Setter
    private static class PlanLine
    {
        private String rawLine;
        private String relation;
        private TableInstanceId id;

        public PlanLine(String rawLine)
        {
            this.rawLine = rawLine;
        }
    }

    private static double getSQLiteVersion()
    {
        JdbcClient jdbcClient = JdbcSupplier.sqlLiteJdbcClientSupplier.get();
        try (Connection connection = jdbcClient.getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery("SELECT SQLITE_VERSION()");
            rs.next();
            return rs.getDouble("SQLITE_VERSION()");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static File[] getQueryFiles(String directoryPath)
    {
        return new File(directoryPath).listFiles((dir, name) -> name.endsWith(".sql"));
    }

    private static String constructQuery(File queryFile)
            throws IOException
    {
        String rawQuery = Files.readString(queryFile.toPath());
        return "EXPLAIN QUERY PLAN " + rawQuery;
    }

    private static void canonicalizeRawPlan(List<PlanLine> rawPlan)
    {
        List<String> imdbTableNames = IMDBDatabase.getTableNames();
        for (String imdbTableName : imdbTableNames) {
            for (PlanLine planLine : rawPlan) {
                List<String> tokens = Arrays.asList(planLine.getRawLine().split(" "));
                // We assume each line either starts with "SCAN TABLE" or "SEARCH TABLE"
                if (tokens.get(2).equals(imdbTableName)) {
                    planLine.setRelation(imdbTableName);
                }
                String aliasID = StringUtils.getDigits(tokens.get(tokens.indexOf("AS") + 1));
                if (!aliasID.isEmpty()) {
                    if (aliasID.equals("1")) {
                        planLine.setId(TableInstanceId.ONE);
                    }
                    else if (aliasID.equals("2")) {
                        planLine.setId(TableInstanceId.TWO);
                    }
                    else {
                        throw new RuntimeException("aliasID not recognized: " + aliasID);
                    }
                }
            }
        }
    }

    private static List<MultiwayJoinNode> getSQLiteJoinOrder(String explainedQuery, JOBQueries queryName)
    {
        List<PlanLine> plan = new ArrayList<>();
        JdbcClient jdbcClient = JdbcSupplier.sqlLiteJdbcClientSupplier.get();
        try (Connection connection = jdbcClient.getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(explainedQuery);
            while (rs.next()) {
                System.out.println("detail= " + rs.getString("detail"));
                plan.add(new PlanLine(rs.getString("detail")));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        canonicalizeRawPlan(plan);
        List<MultiwayJoinNode> joinOrder = new ArrayList<>();
        for (PlanLine planLine : plan) {
            switch (planLine.getRelation()) {
                case akaName:
                    joinOrder.add(getAkaNameInt(queryName));
                    break;
                case akaTitle:
                    joinOrder.add(getAkaTitleInt());
                    break;
                case castInfo:
                    joinOrder.add(getCastInfoInt(queryName));
                    break;
                case charName:
                    joinOrder.add(getCharNameInt(queryName));
                    break;
                case compCastType:
                    joinOrder.add(getCompCastTypeInt(queryName, planLine.getId()));
                    break;
                case companyName:
                    joinOrder.add(getCompanyNameInt(queryName, planLine.getId()));
                    break;
                case companyType:
                    joinOrder.add(getCompanyTypeInt(queryName));
                    break;
                case completeCast:
                    joinOrder.add(getCompleteCastInt());
                    break;
                case infoType:
                    joinOrder.add(getInfoTypeInt(queryName, planLine.getId()));
                    break;
                case keyword:
                    joinOrder.add(getKeywordInt(queryName));
                    break;
                case kindType:
                    joinOrder.add(getKindTypeInt(queryName, planLine.getId()));
                    break;
                case linkType:
                    joinOrder.add(getLinkTypeInt(queryName));
                    break;
                case movieCompanies:
                    joinOrder.add(getMovieCompaniesInt(queryName, planLine.getId()));
                    break;
                case movieInfoIdx:
                    joinOrder.add(getMovieInfoIdxInt(queryName, planLine.getId()));
                    break;
                case movieKeyword:
                    joinOrder.add(getMovieKeywordInt());
                    break;
                case movieLink:
                    joinOrder.add(getMovieLinkInt());
                    break;
                case name:
                    joinOrder.add(getNameInt(queryName));
                    break;
                case roleType:
                    joinOrder.add(getRoleTypeInt(queryName));
                    break;
                case title:
                    if ((queryName.equals(JOBQueries.Q32a) || queryName.equals(JOBQueries.Q32b)) &&
                            planLine.getId() == null) {
                        joinOrder.add(getTitleInt(JOBQueries.Q32, planLine.getId()));
                    }
                    else {
                        joinOrder.add(getTitleInt(queryName, planLine.getId()));
                    }
                    break;
                case movieInfo:
                    joinOrder.add(getMovieInfoInt(queryName));
                    break;
                case personInfo:
                    joinOrder.add(getPersonInfoInt(queryName));
                    break;
            }
        }
        return joinOrder;
    }

    public static String obtainQueryName(File queryFile)
    {
        return "Q" + FilenameUtils.removeExtension(queryFile.getName());
    }

    private static String constructPlanStatisticsQuerySaveName(File queryFile)
    {
        StringBuilder stringBuilder = new StringBuilder();
        String withoutExtension = FilenameUtils.removeExtension(queryFile.getName());
        stringBuilder
                .append("org.zhu45.treetracker.benchmark.job.q")
                .append(StringUtils.getDigits(withoutExtension))
                .append(".Query")
                .append(withoutExtension);
        return stringBuilder.toString();
    }

    private static void getJOBSQLiteJoinOrder()
            throws IOException
    {
        File[] queryFiles = getQueryFiles(jobQueriesPath);
        for (File queryFile : queryFiles) {
            System.out.println("processing " + queryFile.getName() + " ...");
            List<String> queries = constructQueries(queryFile);
            checkState(queries.size() == 2, "there should be two statements");
            String explainQuery = "EXPLAIN QUERY PLAN " + queries.get(1);
            List<MultiwayJoinNode> joinOrder = getSQLiteJoinOrder(explainQuery, JOBQueries.valueOf(obtainQueryName(queryFile)));
            PlanStatistics planStatistics = new PlanStatistics();
            planStatistics.setOptimalJoinOrdering(new JoinOrdering(joinOrder.stream().map(MultiwayJoinNode::getSchemaTableName).collect(Collectors.toList())));
            planStatistics.save(constructPlanStatisticsQuerySaveName(queryFile),
                    JoinOperator.HASH_JOIN,
                    JOB_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH);
        }
    }

    /**
     * Recommend to use "resources/sqlite/tpch/orders.sh" to obtain TPC-H orders due to 15W.sql contains view definition.
     * This function simply executes all the queries under third-party/tpc-h/ and present the output plan.
     * No planStatistics.json will be created.
     */
    private static void getTPCHSQLiteJoinOrder()
            throws IOException
    {
        File[] queryFiles = getQueryFiles(tpchQueriesPath);
        for (File queryFile : queryFiles) {
            System.out.println("processing " + queryFile.getName() + " ...");
            String explainQuery = Files.readString(queryFile.toPath());
            JdbcClient jdbcClient = JdbcSupplier.sqlLiteJdbcClientSupplier.get();
            if (!queryFile.getName().equals("15W.sql")) {
                try (Connection connection = jdbcClient.getConnection();
                        Statement statement = connection.createStatement()) {
                    ResultSet rs = statement.executeQuery(explainQuery);
                    while (rs.next()) {
                        System.out.println(rs.getString("detail"));
                    }
                    System.out.println();
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            else {
                System.out.println("skipping " + queryFile.getName() + " ...\n");
            }
        }
    }

    private static String constructPlanStatisticsQuerySaveNameSSB(File queryFile)
    {
        StringBuilder stringBuilder = new StringBuilder();
        String withoutExtension = FilenameUtils.removeExtension(queryFile.getName());
        stringBuilder
                .append("org.zhu45.treetracker.benchmark.ssb.Query")
                .append(withoutExtension);
        return stringBuilder.toString();
    }

    private static void canonicalizeRawPlanSSB(List<PlanLine> rawPlan)
    {
        List<String> ssbTableNames = SSBDatabase.getTableNames();
        for (String ssbTableName : ssbTableNames) {
            for (PlanLine planLine : rawPlan) {
                List<String> tokens = Arrays.asList(planLine.getRawLine().split(" "));
                // We assume each line either starts with "SCAN TABLE" or "SEARCH TABLE"
                if (tokens.get(2).equals(ssbTableName)) {
                    planLine.setRelation(ssbTableName);
                }
            }
        }
    }

    private static List<MultiwayJoinNode> getSSBSQLiteJoinOrder(String explainedQuery, SSBQueries queryName)
    {
        List<PlanLine> plan = new ArrayList<>();
        JdbcClient jdbcClient = JdbcSupplier.sqlLiteJdbcClientSupplier.get();
        try (Connection connection = jdbcClient.getConnection();
                Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(explainedQuery);
            while (rs.next()) {
                System.out.println("detail= " + rs.getString("detail"));
                plan.add(new PlanLine(rs.getString("detail")));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        canonicalizeRawPlanSSB(plan);
        List<MultiwayJoinNode> joinOrder = new ArrayList<>();
        for (PlanLine planLine : plan) {
            switch (planLine.getRelation()) {
                case customer:
                    joinOrder.add(getCustomerInt(queryName));
                    break;
                case part:
                    joinOrder.add(getPartInt(queryName));
                    break;
                case supplier:
                    joinOrder.add(getSupplierInt(queryName));
                    break;
                case date:
                    joinOrder.add(getDateInt(queryName));
                    break;
                case lineorder:
                    joinOrder.add(getLineOrderInt(queryName));
                    break;
            }
        }
        return joinOrder;
    }

    private static void getSSBSQLiteJoinOrder()
            throws IOException
    {
        File[] queryFiles = getQueryFiles(ssbQueriesPath);
        for (File queryFile : queryFiles) {
            System.out.println("processing " + queryFile.getName() + " ...");
            String explainQuery = constructQuery(queryFile);
            List<MultiwayJoinNode> joinOrder = getSSBSQLiteJoinOrder(explainQuery, SSBQueries.valueOf(obtainQueryName(queryFile)));
            PlanStatistics planStatistics = new PlanStatistics();
            planStatistics.setOptimalJoinOrdering(new JoinOrdering(joinOrder.stream().map(MultiwayJoinNode::getSchemaTableName).collect(Collectors.toList())));
            planStatistics.save(constructPlanStatisticsQuerySaveNameSSB(queryFile),
                    JoinOperator.HASH_JOIN,
                    SSB_SQLITE_ORDERING_STORED_PATH);
        }
    }

    public static void main(String[] args)
            throws IOException
    {
        checkState(3.31 == getSQLiteVersion(), "We assume SQLite 3.31.1 EXPLAIN QUERY PLAN output format");
//        getJOBSQLiteJoinOrder();
//        getTPCHSQLiteJoinOrder();
        getSSBSQLiteJoinOrder();
    }

    private GetSQLiteJoinOrder()
    {
    }
}
