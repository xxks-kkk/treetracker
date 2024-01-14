package org.zhu45.treetracker.jdbc.testing;

import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.BaseJdbcConfig;
import org.zhu45.treetracker.jdbc.ConnectionFactory;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.PostgreSqlClient;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;

/**
 * This is a special JDBC client that is designed to run some specific
 * SQL queries in the data source to verify the correctness of join result.
 */
public class NaturalJoinJdbcClient
        extends PostgreSqlClient
{
    private String sql;

    public NaturalJoinJdbcClient(BaseJdbcConfig config,
                                 ConnectionFactory connectionFactory)
    {
        super(config, connectionFactory);
        sql = "";
    }

    public JdbcTableHandle createNaturalJoinSqlTable(
            String outputSchemaName,
            String outputTableName,
            List<JdbcTableHandle> jdbcTableHandle)
    {
        List<String> catalogs = jdbcTableHandle.stream().map(JdbcTableHandle::getCatalogName).collect(Collectors.toList());
        checkArgument(catalogs.stream().distinct().limit(2).count() <= 1, "provided tables should be under the same catalog");

        try (Connection connection = connectionFactory.openConnection()) {
            String catalog = connection.getCatalog();
            String remoteSchema = toRemoteSchemaName(connection, outputSchemaName);

            this.sql = new NaturalJoinQueryBuilder(identifierQuote).buildSql(
                    catalogs.get(0),
                    jdbcTableHandle.stream().map(handle -> new SchemaTableName(handle.getSchemaName(), handle.getTableName())).collect(Collectors.toList()),
                    quoted(catalog, remoteSchema, outputTableName));
            execute(connection, sql);
            return getTableHandle(new SchemaTableName(outputSchemaName, outputTableName));
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    public String getNaturalJoinSql()
    {
        return this.sql;
    }
}
