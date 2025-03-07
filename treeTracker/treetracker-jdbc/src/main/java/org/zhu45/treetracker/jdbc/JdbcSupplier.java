package org.zhu45.treetracker.jdbc;

import com.google.common.base.Suppliers;
import org.duckdb.DuckDBDriver;
import org.postgresql.Driver;
import org.sqlite.JDBC;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.testing.NaturalJoinJdbcClient;

import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.function.Supplier;

import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.jdbc.DriverConnectionFactory.getInstance;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.getProperties;

/**
 * Provide a JdbcClient to be used throughout different classes.
 */
public class JdbcSupplier
{
    private JdbcSupplier()
    {
    }

    public static Supplier<JdbcClient> postgresJdbcClientSupplier = Suppliers.memoize(() ->
            createTestSqlClient(PostgreSqlClient.class, "db.properties"));

    public static Supplier<JdbcClient> naturalJoinJdbcClientSupplier = Suppliers.memoize(() ->
            createTestSqlClient(NaturalJoinJdbcClient.class, "db.properties"));

    // For benchmarking
    public static Supplier<JdbcClient> duckDBJdbcClientSupplier = Suppliers.memoize(() ->
            createTestSqlClient(DuckDBSqlClient.class, "duckdb.properties"));

    public static Supplier<JdbcClient> sqlLiteJdbcClientSupplier = Suppliers.memoize(() ->
            createTestSqlClient(SQLiteSqlClient.class, "sqlite.properties"));

    // For testing
    public static Supplier<JdbcClient> duckDBTestJdbcClientSupplier = Suppliers.memoize(() ->
            createTestSqlClient(DuckDBSqlClient.class, "duckdb.test.properties"));

    private static JdbcClient createTestSqlClient(Class<? extends BaseJdbcClient> jdbcClientClazz, String dbPropertiesFileName)
    {
        // TODO: After BaseJdbcConfig is introduced, the following code need to overhaul and clean up.
        Properties properties = getProperties(dbPropertiesFileName);
        JdbcIdentity identity = new JdbcIdentity(properties.getProperty("db.user"), properties.getProperty("db.password"));
        BaseJdbcConfig expected = new BaseJdbcConfig()
                .setConnectionUrl(properties.getProperty("db.url"))
                .setConnectionUser(properties.getProperty("db.user"))
                .setConnectionPassword(properties.getProperty("db.password"))
                .setCaseInsensitiveNameMatching(true);
        try {
            Constructor constructor = jdbcClientClazz.getConstructor(BaseJdbcConfig.class, ConnectionFactory.class);
            java.sql.Driver driver = null;
            if (jdbcClientClazz.equals(PostgreSqlClient.class) || jdbcClientClazz.getSuperclass().equals(PostgreSqlClient.class)) {
                driver = new Driver();
            }
            else if (jdbcClientClazz.equals(DuckDBSqlClient.class)) {
                driver = new DuckDBDriver();
            }
            else if (jdbcClientClazz.equals(SQLiteSqlClient.class)) {
                driver = new JDBC();
            }
            return (JdbcClient) constructor.newInstance(
                    expected,
                    getInstance(driver,
                            properties.getProperty("db.url"), identity));
        }
        catch (Exception e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
        }
    }
}
