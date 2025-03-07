package org.zhu45.treetracker.jdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SQLiteSqlClient
        extends BaseJdbcClient
{
    private static final Logger log = LogManager.getLogger(SQLiteSqlClient.class);

    public SQLiteSqlClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(new JdbcConnectorId("sqlite"), config, "\"", connectionFactory);
    }
}
