package org.zhu45.treetracker.jdbc.testing;

import java.sql.Connection;

public interface IdleConnectionCounter
{
    /**
     * Count the number of idle connections.
     *
     * @param connection current JDBC connection to be used for querying the number of idle connections.
     * @return idle connection count.
     */
    int count(Connection connection);
}
