package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

public interface JdbcClient
{
    JdbcOutputTableHandle createTable(String schemaName, Relation relation);

    Connection getConnection()
            throws SQLException;

    void dropTable(JdbcTableHandle handle);

    // delete all rows from table
    void deleteTable(JdbcTableHandle handle);

    void insertTable(String schemaName, Relation relation);

    // Load data from external CSV file
    void loadTable(SchemaTableName schemaTableName, String filePath, String delimiter);

    JdbcTableHandle getTableHandle(SchemaTableName schemaTableName);

    List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle);

    Optional<ReadMapping> toTreeTrackerType(JdbcTypeHandle typeHandle);

    PreparedStatement buildSql(Connection connection, List<JdbcColumnHandle> columnHandles, JdbcTableHandle jdbcTableHandle)
            throws SQLException;

    int getTableSize(Connection connection, JdbcTableHandle jdbcTableHandle);

    PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException;

    List<SchemaTableName> getTableNames(Optional<String> schema);

    void ingestRelation(String schemaName,
            String relationName,
            List<String> attributes,
            List<Type> types,
            List<List<RelationalValue>> vals);

    List<String> getAttributes(SchemaTableName schemaTableName);

    void createSchema(String schemaName);

    default void abortReadConnection(Connection connection)
            throws SQLException
    {
        connection.close();
    }

    // get the percentage of values in a column that is unique
    float fractionOfUniqueValuesInColumn(JdbcTableHandle jdbcTableHandle, List<JdbcColumnHandle> columnHandles);
}
