package org.zhu45.treetracker.jdbc;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.TreeTrackerException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

import static java.lang.String.format;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;

public class DuckDBSqlClient
        extends BaseJdbcClient
{
    private static final Logger log = LogManager.getLogger(DuckDBSqlClient.class);

    public DuckDBSqlClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(new JdbcConnectorId("duckdb"), config, "\"", connectionFactory);
    }

    @Override
    public void createSchema(String schemaName)
    {
        try (Connection connection = connectionFactory.openConnection();
                Statement statement = connection.createStatement()) {
            String remoteSchema = toRemoteSchemaName(connection, schemaName);
            String catalog = connection.getCatalog();
            String sql = format(
                    "CREATE SCHEMA IF NOT EXISTS %s.%s",
                    quoted(catalog), quoted(remoteSchema));
            if (Switches.DEBUG) {
                log.debug("create schema sql: " + sql);
            }
            statement.execute(sql);
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"BASE TABLE", "VIEW"});
    }
}
