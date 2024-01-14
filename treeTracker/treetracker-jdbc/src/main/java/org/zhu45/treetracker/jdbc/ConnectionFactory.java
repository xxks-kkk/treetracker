package org.zhu45.treetracker.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionFactory
        extends AutoCloseable
{
    Connection openConnection()
            throws SQLException;

    @Override
    default void close()
            throws SQLException
    {
    }
}
