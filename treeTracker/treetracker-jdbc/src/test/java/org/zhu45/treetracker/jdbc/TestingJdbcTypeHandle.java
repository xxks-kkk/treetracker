package org.zhu45.treetracker.jdbc;

import java.sql.Types;

public final class TestingJdbcTypeHandle
{
    private TestingJdbcTypeHandle() {}

    public static final JdbcTypeHandle JDBC_CHAR = new JdbcTypeHandle(Types.CHAR, 10, 0);
    public static final JdbcTypeHandle JDBC_INTEGER = new JdbcTypeHandle(Types.INTEGER, 4, 0);
}
