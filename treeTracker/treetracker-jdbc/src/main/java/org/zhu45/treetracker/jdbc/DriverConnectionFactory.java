package org.zhu45.treetracker.jdbc;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;

public class DriverConnectionFactory
        implements ConnectionFactory
{
    private static final LoggerProvider.TreeTrackerLogger traceLogger = getLogger(DriverConnectionFactory.class);
    private static final Logger logger = LogManager.getLogger(DriverConnectionFactory.class.getName());

    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;

    private ConnectionLeakUtil connectionLeakUtil;

    private static DriverConnectionFactory driverConnectionFactory;

    public static Properties basicConnectionProperties(JdbcIdentity identity)
    {
        Properties connectionProperties = new Properties();
        if (identity.getConnectionUser() != null) {
            connectionProperties.setProperty("user", identity.getConnectionUser());
        }
        if (identity.getConnectionPassword() != null) {
            connectionProperties.setProperty("password", identity.getConnectionPassword());
        }
        return connectionProperties;
    }

    private DriverConnectionFactory(Driver driver, String connectionUrl, JdbcIdentity identity)
    {
        this.driver = driver;
        this.connectionProperties = basicConnectionProperties(identity);
        this.connectionUrl = requireNonNull(connectionUrl, "connectionUrl is null");
        if (Switches.DEBUG) {
            connectionLeakUtil = new ConnectionLeakUtil();
            traceLogger.debugBlue("initial connect leak count: " + connectionLeakUtil.getConnectionLeakCount());
        }
    }

    public static DriverConnectionFactory getInstance(Driver driver, String connectionUrl, JdbcIdentity identity)
    {
        // In mvn test, we may have different connectionUrl due to different JdbcClient we use
        if (driverConnectionFactory == null || !Objects.equals(driverConnectionFactory.connectionUrl, connectionUrl)) {
            driverConnectionFactory = new DriverConnectionFactory(driver, connectionUrl, identity);
        }
        return driverConnectionFactory;
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        Connection connection = driver.connect(this.connectionUrl, connectionProperties);
        if (Switches.DEBUG) {
            if (traceLogger.isDebugEnabled()) {
                traceLogger.debugBlue("current connection leak count: " + connectionLeakUtil.countConnectionLeaks());
                traceLogger.debugRed("open a connection", 5);
            }
            if (logger.isTraceEnabled()) {
                logger.trace("open a connection");
                logger.trace("current connection leak count: " + connectionLeakUtil.countConnectionLeaks());
            }
            checkState(connection != null, "Driver returned null connection");
        }
        return connection;
    }
}
