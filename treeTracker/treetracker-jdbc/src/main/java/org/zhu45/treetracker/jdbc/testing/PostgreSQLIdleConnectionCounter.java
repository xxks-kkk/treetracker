package org.zhu45.treetracker.jdbc.testing;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLIdleConnectionCounter
        implements IdleConnectionCounter
{
    public static final IdleConnectionCounter INSTANCE =
            new PostgreSQLIdleConnectionCounter();

    @Override
    public int count(Connection connection)
    {
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(
                    "SELECT COUNT(*) " +
                            "FROM pg_stat_activity " +
                            "WHERE state ILIKE '%idle%'")) {
                while (resultSet.next()) {
                    return resultSet.getInt(1);
                }
                return 0;
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
