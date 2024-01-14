package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.common.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class QueryBuilder
{
    private final String quote;

    public QueryBuilder(String quote)
    {
        this.quote = requireNonNull(quote, "quote is null");
    }

    public PreparedStatement buildSql(
            JdbcClient client,
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<JdbcColumnHandle> columns)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = columns.stream()
                .map(JdbcColumnHandle::getColumnName)
                .map(this::quote)
                .collect(joining(", "));

        sql.append("SELECT ");
        sql.append(columnNames);
        if (columns.isEmpty()) {
            sql.append("null");
        }

        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        return client.getPreparedStatement(connection, sql.toString());
    }

    public PreparedStatement getTableSize(
            JdbcClient client,
            Connection connection,
            String catalog,
            String schema,
            String table)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        sql.append("SELECT COUNT(*)");
        sql.append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        return client.getPreparedStatement(connection, sql.toString());
    }

    public PreparedStatement getNumUniqueForColumn(
            JdbcClient client,
            Connection connection,
            String catalog,
            String schema,
            String table,
            List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder();

        String columnNames = columnHandles.stream()
                .map(JdbcColumnHandle::getColumnName)
                .map(this::quote)
                .collect(joining(", "));

        sql.append("SELECT COUNT(DISTINCT ")
                .append("(")
                .append(columnNames)
                .append(")")
                .append(")")
                .append(" FROM ");
        if (!isNullOrEmpty(catalog)) {
            sql.append(quote(catalog)).append('.');
        }
        if (!isNullOrEmpty(schema)) {
            sql.append(quote(schema)).append('.');
        }
        sql.append(quote(table));

        return client.getPreparedStatement(connection, sql.toString());
    }

    private static boolean isAcceptedType(Type type)
    {
        Type validType = requireNonNull(type, "type is null");
        return validType instanceof CharType;
    }

    protected String quote(String name)
    {
        name = name.replace(quote, quote + quote);
        return quote + name + quote;
    }
}
