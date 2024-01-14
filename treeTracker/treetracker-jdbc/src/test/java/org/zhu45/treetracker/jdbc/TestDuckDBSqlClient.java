package org.zhu45.treetracker.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.zhu45.treetracker.common.ColorValue;
import org.zhu45.treetracker.common.Column;
import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.common.type.CharType.createCharType;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBTestJdbcClientSupplier;

@DisabledIfEnvironmentVariable(named=GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestDuckDBSqlClient
{
    private static final String schemaName = "public";
    private static final String tableName = "L1";
    private JdbcClient jdbcClient;
    private ConnectionLeakUtil connectionLeakUtil;

    @BeforeAll
    public void setUp() {
        jdbcClient = duckDBTestJdbcClientSupplier.get();
        connectionLeakUtil = new ConnectionLeakUtil();
        jdbcClient.createSchema(schemaName);
    }

    @Test
    public void testGetConnection() throws SQLException
    {
        // Note that using try with resource pattern doesn't work in this case
        // because JdbcClient doesn't implement AutoCloseable or Closeable interface.
        // This is a common fallacy. If we want to use try with resource pattern, use
        // DriverManager from Java or our ConnectionFactory, which implements AutoCloseable interface.
        try (Connection connection = jdbcClient.getConnection()) {
            assertNotNull(connection);
        }
    }

    @Test
    public void testCreateTable() {
        Column color = new Column("color", createCharType(10), new ArrayList());
        Relation relation = new Relation(tableName, singletonList(color));
        try {
            assertNotNull(jdbcClient.createTable(schemaName, relation));
        } finally {
            JdbcTableHandle tableHandle = jdbcClient.getTableHandle(new SchemaTableName(schemaName, tableName));
            if (tableHandle != null) {
                jdbcClient.dropTable(tableHandle);
            }
        }
    }

    @Test
    public void testInsertTable() {
        CharType charType = createCharType(10);
        List<RelationalValue> colors = List.of(
                new ColorValue(charType, ColorValue.Color.Black),
                new ColorValue(charType, ColorValue.Color.Grey),
                new ColorValue(charType, ColorValue.Color.Red),
                new ColorValue(charType, ColorValue.Color.Orange),
                new ColorValue(charType, ColorValue.Color.Blue));
        Column color = new Column("color", createCharType(10), colors);
        Relation relation = new Relation(tableName, singletonList(color));
        try {
            jdbcClient.createTable(schemaName, relation);
            jdbcClient.insertTable(schemaName, relation);
        } finally {
            JdbcTableHandle tableHandle = jdbcClient.getTableHandle(new SchemaTableName(schemaName, tableName));
            jdbcClient.dropTable(tableHandle);
        }
    }

    @Test
    public void testInsertTable2() throws SQLException {
        // TODO: refactor the test case to query the postgres and assert on result set
        try {
            createTestTable();
        } finally {
            JdbcTableHandle tableHandle = jdbcClient.getTableHandle(new SchemaTableName(schemaName, tableName));
            jdbcClient.dropTable(tableHandle);
        }
    }

    @Test
    public void testGetColumns() {
        JdbcOutputTableHandle outputTableHandle = createTestTable();
        JdbcTableHandle jdbcTableHandle = new JdbcTableHandle(
                outputTableHandle.getConnectorId(),
                new SchemaTableName(outputTableHandle.getSchemaName(), outputTableHandle.getTableName()),
                outputTableHandle.getCatalogName(),
                outputTableHandle.getSchemaName(),
                outputTableHandle.getTableName()
        );
        try {
            List<JdbcColumnHandle> jdbcColumnHandles = jdbcClient.getColumns(jdbcTableHandle);
            assertEquals(jdbcColumnHandles.size(), 2);
        } finally {
            jdbcClient.dropTable(jdbcTableHandle);
        }
    }

    private JdbcOutputTableHandle createTestTable() {
        CharType type = createCharType(10);
        List<RelationalValue> colors = List.of(
                new ColorValue(type, ColorValue.Color.Black),
                new ColorValue(type, ColorValue.Color.Grey),
                new ColorValue(type, ColorValue.Color.Red),
                new ColorValue(type, ColorValue.Color.Orange),
                new ColorValue(type, ColorValue.Color.Blue));
        Column color = new Column("color", type, colors);
        List<RelationalValue> names = List.of(
                new StringValue(type,"dan"),
                new StringValue(type,"zeyuan"),
                new StringValue(type, "tiratat"),
                new StringValue(type, "lokendra"),
                new StringValue(type, "anu"));
        Column name = new Column("name", type, names);
        Relation relation = new Relation(tableName, new ArrayList<>(Arrays.asList(color, name)));
        JdbcOutputTableHandle handle = jdbcClient.createTable(schemaName, relation);
        jdbcClient.insertTable(schemaName, relation);
        return handle;
    }

    /**
     * This is used to demonstrate a feature of DuckDB where the in-memory database
     * is gone once the connection is closed, i.e., open a different connection means
     * open a new different in-memory database. Online documentation: https://duckdb.org/docs/api/java
     * suggests that in order to spawn multiple connections, we need to use
     * Connection conn2 = ((DuckDBConnection) conn).duplicate();
     */
    @Test
    public void inMemorySettingHasToKeepTheSameConnection()
    {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS \"memory\".\"public\"");
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
           assertThrows(SQLException.class, () -> stmt.execute("CREATE TABLE \"memory\".\"public\".\"L1\" (\"color\" char(10) NOT NULL)"));
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public void assertNoLeaks() {
        connectionLeakUtil.assertNoLeaks();
    }
}
