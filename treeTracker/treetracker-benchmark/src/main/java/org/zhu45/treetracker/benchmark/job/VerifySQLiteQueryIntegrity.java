package org.zhu45.treetracker.benchmark.job;

import org.apache.commons.lang3.StringUtils;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcSupplier;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.benchmark.codegen.GetSQLiteJoinOrder.getQueryFiles;
import static org.zhu45.treetracker.benchmark.codegen.GetSQLiteJoinOrder.jobQueriesPath;
import static org.zhu45.treetracker.benchmark.codegen.GetSQLiteJoinOrder.obtainQueryName;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

/**
 * We aim to verify the SQLite JOB queries return the same result set
 * as the other JOB implementation.
 *
 * There seems to be some bug with JDBC execution, e.g., 10c using JDBC returns 26 but
 * using SQLite shell returns 10. The later one is correct.
 * Same issue happens to 15c, 15d, 19a, 19c, 20a, 24a, 24b, 29c, 7c, 9a, 13b, 17d,
 * 17f, 17b, 17a, 9b, 18a, 8a, 9c, 7a.
 */
public class VerifySQLiteQueryIntegrity
{
    private VerifySQLiteQueryIntegrity() {}

    public static List<String> constructQueries(File queryFile)
            throws IOException
    {
        List<String> returnQueries = new ArrayList<>();
        String rawQuery = Files.readString(queryFile.toPath());
        int startOfSelect = StringUtils.indexOfIgnoreCase(rawQuery, "select");
        int endOfSelect = StringUtils.indexOfIgnoreCase(rawQuery, "select") + "select".length();
        int startOfFrom = StringUtils.indexOfIgnoreCase(rawQuery, "from");
        returnQueries.add("PRAGMA case_sensitive_like=ON;");
        StringBuilder builder = new StringBuilder();
        builder.append(rawQuery, startOfSelect, endOfSelect)
                .append(" count(*) ")
                .append(rawQuery.substring(startOfFrom));
        returnQueries.add(builder.toString());
        return returnQueries;
    }

    public static void run(List<String> excludeQueries)
            throws IOException
    {
        File[] queries = getQueryFiles(jobQueriesPath);
        int totalExamined = 0;
        int correct = 0;
        for (File queryFile : queries) {
            System.out.println("processing " + queryFile.getName() + " ...");
            if (excludeQueries.contains(queryFile.getName())) {
                System.out.println("exclude: " + queryFile.getName());
                continue;
            }
            totalExamined += 1;
            JOBQueries queryName = JOBQueries.valueOf(obtainQueryName(queryFile));
            List<String> queryStmts = constructQueries(queryFile);
            JdbcClient jdbcClient = JdbcSupplier.sqlLiteJdbcClientSupplier.get();
            checkState(queryStmts.size() <= 2, "There should be at most two statements");
            long resultSetSize = 0;
            for (int i = 0; i < queryStmts.size(); i++) {
                try (Connection connection = jdbcClient.getConnection();
                        Statement statement = connection.createStatement()) {
                    if (i == 1) {
                        ResultSet rs = statement.executeQuery(queryStmts.get(i));
                        // Assume the second statement is the select statement
                        rs.next();
                        resultSetSize = Long.parseLong(rs.getString(1));
                    }
                    else {
                        statement.execute(queryStmts.get(i));
                    }
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            JoinFragmentType query = queryProvider(JoinOperator.HASH_JOIN,
                    queryName,
                    List.of(),
                    duckDBJdbcClientSupplier.get());
            long expectedResultSetSize = query.evalWithResultSize();
            if (expectedResultSetSize == resultSetSize) {
                System.out.printf("%s confirm correct: expected: %s actual: %s\n",
                        queryFile.getName(), expectedResultSetSize, resultSetSize);
                correct += 1;
            }
            else {
                System.out.printf("%s incorrect: expected: %s actual: %s%n", queryFile.getName(),
                        expectedResultSetSize, resultSetSize);
            }
        }
        System.out.printf("total checked: %s correct: %s\n", totalExamined, correct);
    }

    public static void main(String[] args)
            throws IOException
    {
        run(List.of());
    }
}
