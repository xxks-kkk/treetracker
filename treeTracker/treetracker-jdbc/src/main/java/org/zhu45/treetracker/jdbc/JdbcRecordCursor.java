package org.zhu45.treetracker.jdbc;

import com.google.common.base.VerifyException;
import de.renebergelt.test.Switches;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;

public class JdbcRecordCursor
        implements RecordCursor
{
    private static final LoggerProvider.TreeTrackerLogger log = getLogger(JdbcRecordCursor.class);

    private final JdbcColumnHandle[] columnHandles;
    private final StringReadFunction[] stringReadFunctions;
    private final LongReadFunction[] longReadFunctions;

    private JdbcClient jdbcClient;
    private Connection connection;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private boolean closed;

    public JdbcRecordCursor(JdbcClient jdbcClient, List<JdbcColumnHandle> columnHandles, JdbcTableHandle jdbcTableHandle)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        requireNonNull(jdbcTableHandle, "jdbcTableHandle is null");

        this.columnHandles = columnHandles.toArray(new JdbcColumnHandle[0]);

        stringReadFunctions = new StringReadFunction[columnHandles.size()];
        longReadFunctions = new LongReadFunction[columnHandles.size()];

        for (int i = 0; i < this.columnHandles.length; i++) {
            ReadMapping readMapping = jdbcClient.toTreeTrackerType(columnHandles.get(i).getJdbcTypeHandle())
                    .orElseThrow(() -> new VerifyException("Unsupported column type"));
            Class<?> javaType = readMapping.getType().getJavaType();
            ReadFunction readFunction = readMapping.getReadFunction();

            if (javaType == String.class) {
                stringReadFunctions[i] = (StringReadFunction) readFunction;
            }
            else if (javaType == long.class) {
                longReadFunctions[i] = (LongReadFunction) readFunction;
            }
            else {
                throw new IllegalStateException(format("Unsupported java type %s", javaType));
            }
        }

        try {
            connection = jdbcClient.getConnection();
            // We only support SELECT * FROM statement without predicate.
            statement = jdbcClient.buildSql(connection, columnHandles, jdbcTableHandle);
            resultSet = statement.executeQuery();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }

        try {
            return resultSet.next();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public void reset()
    {
        closed = false;
        if (Switches.DEBUG) {
            try {
                if (Switches.DEBUG && log.isDebugEnabled()) {
                    log.debug("reset() is called");
                }
                resultSet.beforeFirst();
            }
            catch (SQLFeatureNotSupportedException ignored) {
                // FIXME: This code path hits when we use DuckDB JDBC because it doesn't
                // support beforeFirst() (0.8.0 version). In fact, we don't need to call beforeFirst()
                // for TTJ and Hash Join. The only place it gets used is for nested-loop join and
                // TT-2 algorithm. Thus, we only need this for build test not benchmarking. As a workaround,
                // we could use CachedRowSet, which is supported by DuckDB JDBC but it's much effort and
                // the gain is not relevant to our goal. In the long run, we probably need to have a connector model
                // to fill the gap on the differences of each vendor JDBC implementations.
            }
            catch (SQLException e) {
                throw handleSqlException(e);
            }
        }
    }

    @Override
    public String getString(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return stringReadFunctions[field].readString(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        try {
            return longReadFunctions[field].readLong(resultSet, field + 1);
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles[field].getColumnType();
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.length, "Invalid field index");

        try {
            // JDBC is kind of dumb: we need to read the field and then ask
            // if it was null, which means we are wasting effort here.
            // We could save the result of the field access if it matters.
            resultSet.getObject(field + 1);

            return resultSet.wasNull();
        }
        catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        // use try with resources to close everything properly
        try (Connection connection = this.connection;
                Statement statement = this.statement;
                ResultSet resultSet = this.resultSet) {
            statement.close();
            resultSet.close();
            jdbcClient.abortReadConnection(connection);
            checkState(connection.isClosed());
        }
        catch (SQLException e) {
            // ignore exception from close
        }
        connection = null;
        statement = null;
        resultSet = null;
        jdbcClient = null;
    }

    @Override
    public boolean hasNext()
    {
        try {
            if (Switches.DEBUG && log.isDebugEnabled()) {
                log.debug("!resultSet.isLast(): " + !resultSet.isLast());
                log.debug("(resultSet.getRow() != 0): " + (resultSet.getRow() != 0));
                log.debug("resultSet.isBeforeFirst(): " + resultSet.isBeforeFirst());
                log.debug("Thus, hasNext() is: " + (!resultSet.isLast() && ((resultSet.getRow() != 0) || resultSet.isBeforeFirst())));
            }
            return (!resultSet.isLast() && ((resultSet.getRow() != 0) || resultSet.isBeforeFirst()));
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public int numColumns()
    {
        return columnHandles.length;
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new TreeTrackerException(JDBC_ERROR, e);
    }
}
