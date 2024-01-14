package org.zhu45.treetracker.relational;

import org.zhu45.treetracker.jdbc.JdbcClient;

public interface RuntimeContext
{
    JdbcClient getJdbcClient();
    String getSchema();
}
