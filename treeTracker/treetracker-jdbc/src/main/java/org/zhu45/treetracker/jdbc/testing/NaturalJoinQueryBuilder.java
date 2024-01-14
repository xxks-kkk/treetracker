package org.zhu45.treetracker.jdbc.testing;

import com.google.common.base.Joiner;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.QueryBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;

public class NaturalJoinQueryBuilder
        extends QueryBuilder
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(NaturalJoinQueryBuilder.class);

    public NaturalJoinQueryBuilder(String quote)
    {
        super(quote);
    }

    public String buildSql(
            String catalog,
            List<SchemaTableName> schemaTableNames,
            String outputSchemaTableName)
    {
        StringBuilder sql = new StringBuilder();

        sql.append("CREATE TABLE ")
                .append(outputSchemaTableName)
                .append("as (");

        sql.append("SELECT ");
        sql.append("*");

        sql.append(" FROM ");

        List<String> fullQualifiedTables = new ArrayList<>();
        for (SchemaTableName schemaTableName : schemaTableNames) {
            StringBuilder fullQualifiedTable = new StringBuilder();
            if (!isNullOrEmpty(catalog)) {
                fullQualifiedTable.append(quote(catalog)).append('.');
            }
            fullQualifiedTable.append(quote(schemaTableName.getSchemaName())).append('.');
            fullQualifiedTable.append(quote(schemaTableName.getTableName()));
            fullQualifiedTables.add(fullQualifiedTable.toString());
        }

        sql.append(Joiner.on(" natural join ").join(fullQualifiedTables));
        sql.append(")");

        // Example SQL produced:
        // CREATE TABLE "postgres"."multiway"."naturalJoinTable" as
        // (SELECT * FROM "multiway"."U" natural join "multiway"."T" natural join "multiway"."A"
        // natural join "multiway"."S" natural join "multiway"."R" natural join "multiway"."B")
        log.infoBlueBoldBright("Postgres SQL: " + sql.toString());
        return sql.toString();
    }

    public PreparedStatement buildExplainSql(
            JdbcClient client,
            Connection connection,
            Collection<SchemaTableName> schemaTableNames,
            boolean useTrueCard)
            throws SQLException
    {
        String explainHeader = useTrueCard ? "EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON, ANALYZE) " :
                "EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON) ";
        StringBuilder sql = new StringBuilder(explainHeader);

        sql.append("SELECT ")
                .append("*")
                .append(" FROM ");

        String catalog = connection.getCatalog();
        List<String> fullQualifiedTables = new ArrayList<>();
        for (SchemaTableName schemaTableName : schemaTableNames) {
            StringBuilder fullQualifiedTable = new StringBuilder();
            if (!isNullOrEmpty(catalog)) {
                fullQualifiedTable.append(quote(catalog)).append('.');
            }
            fullQualifiedTable.append(quote(schemaTableName.getSchemaName())).append('.');
            fullQualifiedTable.append(quote(schemaTableName.getTableName()));
            fullQualifiedTables.add(fullQualifiedTable.toString());
        }

        sql.append(Joiner.on(" natural join ").join(fullQualifiedTables));
        return client.getPreparedStatement(connection, sql.toString());
    }
}
