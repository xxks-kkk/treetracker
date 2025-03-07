package org.zhu45.treetracker.benchmark.codegen;

import com.google.common.base.Joiner;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FilenameUtils;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcSupplier;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.benchmark.Benchmarks.ROOT;
import static org.zhu45.treetracker.benchmark.job.VerifySQLiteQueryIntegrity.constructQueries;

/**
 * We aim to generate JOB queries for Postgres with pg_hint_plan hints
 * to enforce Postgres using SQLite join order with left-deep plan.
 */
public class GenerateJOBForPostgresBaseOnSQLiteOrder
{
    private static final String thirdPartyPath;
    public static final String jobQueriesPath;
    public static final String jobQueriesPathPostgres;
    public static final String jobQueriesPathPostgresEnforced;

    static {
        thirdPartyPath = Paths.get(ROOT, "third-party").toString();
        jobQueriesPath = Paths.get(thirdPartyPath, "join-order-benchmark").toString();
        jobQueriesPathPostgres = Paths.get(thirdPartyPath, "join-order-benchmark-postgres").toString();
        jobQueriesPathPostgresEnforced = Paths.get(thirdPartyPath, "join-order-benchmark-postgres-enforced").toString();
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
        try (Connection connection = jdbcClient.getConnection(); Statement statement = connection.createStatement()) {
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

    private static String constructQuery(File queryFile) throws IOException
    {
        String rawQuery = Files.readString(queryFile.toPath());
        return "EXPLAIN QUERY PLAN " + rawQuery;
    }

    private static void canonicalizeRawPlan(List<PlanLine> rawPlan)
    {
        for (PlanLine planLine : rawPlan) {
            List<String> tokens = Arrays.asList(planLine.getRawLine().split(" "));
            planLine.setRelation(tokens.get(tokens.indexOf("AS") + 1));
        }
    }

    private static List<String> getSQLiteJoinOrder(String explainedQuery, JOBQueries queryName)
    {
        List<PlanLine> plan = new ArrayList<>();
        JdbcClient jdbcClient = JdbcSupplier.sqlLiteJdbcClientSupplier.get();
        try (Connection connection = jdbcClient.getConnection(); Statement statement = connection.createStatement()) {
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
        return plan.stream().map(planLine -> planLine.relation).collect(Collectors.toList());
    }

    public static String obtainQueryName(File queryFile)
    {
        return "Q" + FilenameUtils.removeExtension(queryFile.getName());
    }

    private static void generateJOBPostgresQueries(List<String> joinOrder, File queryFile) throws IOException
    {
        File postgresQueryFile = requireNonNull(
                new File(jobQueriesPathPostgres).listFiles((dir, name) -> name.equals(queryFile.getName())))[0];
        String rawQuery = Files.readString(postgresQueryFile.toPath());
        StringBuilder queryBuilder = new StringBuilder();
        StringBuilder hintJoinOrder = new StringBuilder();
        hintJoinOrder.append("(".repeat(joinOrder.size() - 1));
        hintJoinOrder.append(joinOrder.get(0)).append(" ").append(joinOrder.get(1)).append(")");
        for (int i = 2; i < joinOrder.size(); i++) {
            hintJoinOrder.append(" ").append(joinOrder.get(i)).append(")");
        }
        List<String> hints = List.of(String.format("/*+ Leading( %s )", hintJoinOrder.toString()),
                                     String.format("    HashJoin(%s)", Joiner.on(" ").join(joinOrder)), "*/");
        queryBuilder.append(Joiner.on("\n").join(hints)).append("\n").append(rawQuery);
        Path newQueryPath = Paths.get(jobQueriesPathPostgresEnforced, queryFile.getName());
        Files.writeString(newQueryPath, queryBuilder.toString());
        System.out.println("write to: " + newQueryPath);
    }

    private static void generateJOBQueriesForPostgresWithSQLiteOrder() throws IOException
    {
        File[] queryFiles = getQueryFiles(jobQueriesPath);
        for (File queryFile : queryFiles) {
            System.out.println("processing " + queryFile.getName() + " ...");
            List<String> queries = constructQueries(queryFile);
            checkState(queries.size() == 2, "there should be two statements");
            String explainQuery = "EXPLAIN QUERY PLAN " + queries.get(1);
            List<String> joinOrder = getSQLiteJoinOrder(explainQuery, JOBQueries.valueOf(obtainQueryName(queryFile)));
            generateJOBPostgresQueries(joinOrder, queryFile);
        }
    }

    public static void main(String[] args) throws IOException
    {
        checkState(3.31 == getSQLiteVersion(), "We assume SQLite 3.31.1 EXPLAIN QUERY PLAN output format");
        generateJOBQueriesForPostgresWithSQLiteOrder();
    }

    private GenerateJOBForPostgresBaseOnSQLiteOrder()
    {
    }
}
