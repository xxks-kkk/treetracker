package org.zhu45.treetracker.jdbc.testing;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.zhu45.treetracker.jdbc.testing.TestUtils.getProperties;

public class ConnectionLeakUtil
{
    private Properties properties = getProperties("db.properties");

    private IdleConnectionCounter connectionCounter;

    private int connectionLeakCount;

    public ConnectionLeakUtil()
    {
        List<IdleConnectionCounter> idleConnectionCounters = Collections.singletonList(PostgreSQLIdleConnectionCounter.INSTANCE);

        for (IdleConnectionCounter connectionCounter : idleConnectionCounters) {
            this.connectionCounter = connectionCounter;
            break;
        }
        if (connectionCounter != null) {
            connectionLeakCount = countConnectionLeaks();
        }
    }

    public void assertNoLeaks()
    {
        if (connectionCounter != null) {
            int currentConnectionLeakCount = countConnectionLeaks();
            int diff = currentConnectionLeakCount - connectionLeakCount;
            if (diff > 0) {
                throw new RuntimeException(
                        String.format(
                                "%d connection(s) have been leaked! Previous leak count: %d, Current leak count: %d",
                                diff,
                                connectionLeakCount,
                                currentConnectionLeakCount));
            }
        }
    }

    public int countConnectionLeaks()
    {
        try (Connection connection = newConnection()) {
            return connectionCounter.count(connection);
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private Connection newConnection()
    {
        try {
            return DriverManager.getConnection(
                    properties.getProperty("db.url"),
                    properties.getProperty("db.user"),
                    properties.getProperty("db.password"));
        }
        catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public int getConnectionLeakCount()
    {
        return connectionLeakCount;
    }
}
