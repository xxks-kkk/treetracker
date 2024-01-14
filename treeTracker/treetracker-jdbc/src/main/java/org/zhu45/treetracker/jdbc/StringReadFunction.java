package org.zhu45.treetracker.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface StringReadFunction
        extends ReadFunction
{
    @Override
    default Class<?> getJavaType()
    {
        return String.class;
    }

    String readString(ResultSet resultSet, int columnIndex)
            throws SQLException;
}
