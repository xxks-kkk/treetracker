package org.zhu45.treetracker.jdbc;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.Column;
import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TableNotFoundException;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.common.type.IntegerType;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.common.type.VarcharType;

import javax.annotation.Nullable;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.ResultSetMetaData.columnNullable;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_NOT_IMPLEMENTED;
import static org.zhu45.treetracker.common.StandardErrorCode.NOT_SUPPORTED;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.isVarcharType;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;

public class BaseJdbcClient
        implements JdbcClient
{
    private static final Logger log = LogManager.getLogger(BaseJdbcClient.class);

    private static final Map<Type, String> SQL_TYPES = ImmutableMap.<Type, String>builder()
            .put(INTEGER, "integer")
            .build();

    // The maximum vals, i.e., number of rows x vals per row, to be allowed using JDBC insertion;
    // If the number goes beyond this bound, we generate CSV into local and ingest using Postgres COPY
    private static final long maximumValsToBeAllowedUsingJDBCInsert = 4194304;

    protected final String connectorId;
    protected final ConnectionFactory connectionFactory;
    protected final boolean caseInsensitiveNameMatching;
    protected final String identifierQuote;

    private Connection connection;

    public BaseJdbcClient(JdbcConnectorId connectorId,
                          BaseJdbcConfig config,
                          String identifierQuote,
                          ConnectionFactory connectionFactory)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        this.caseInsensitiveNameMatching = config.isCaseInsensitiveNameMatching();
        this.identifierQuote = requireNonNull(identifierQuote, "identifierQuote is null");
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        if (connection == null || connection.isClosed()) {
            connection = connectionFactory.openConnection();
        }
        return connection;
    }

    @Override
    public void dropTable(JdbcTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("DROP TABLE ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        try (Connection connection = connectionFactory.openConnection()) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public void deleteTable(JdbcTableHandle handle)
    {
        StringBuilder sql = new StringBuilder()
                .append("DELETE FROM ")
                .append(quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()));

        try (Connection connection = connectionFactory.openConnection()) {
            execute(connection, sql.toString());
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    private long numberOfValsForRows(Relation relation)
    {
        List<Row> rows = relation.getRows();
        if (rows.size() > 0) {
            Row row = rows.get(0);
            return (long) rows.size() * row.size();
        }
        return 0;
    }

    protected boolean numberOfValsSmallerThanMaximum(Relation relation)
    {
        long numberOfVals = numberOfValsForRows(relation);
        return numberOfVals < maximumValsToBeAllowedUsingJDBCInsert;
    }

    protected void insertTableJdbc(String schemaName, Relation relation)
    {
        String tableName = relation.getRelationName();
        try (Connection connection = connectionFactory.openConnection()) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(connection, schemaName);
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();
            String sql = buildInsertSql(relation.getRows(), catalog, remoteSchema, tableName);
            log.debug(String.format("insert sql: %s", sql));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public void insertTable(String schemaName, Relation relation)
    {
        checkArgument(numberOfValsSmallerThanMaximum(relation),
                String.format("Too many vals: %s to use JDBC insertion!", numberOfValsForRows(relation)));
        insertTableJdbc(schemaName, relation);
    }

    @Override
    public void loadTable(SchemaTableName schemaTableName, String filePath, String delimiter)
    {
        throw new TreeTrackerException(FUNCTION_NOT_IMPLEMENTED, "loadTable should be implemented by the subclass");
    }

    @Override
    public JdbcOutputTableHandle createTable(String schemaName, Relation relation)
    {
        String tableName = relation.getRelationName();
        try (Connection connection = connectionFactory.openConnection()) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(connection, schemaName);
            String remoteTable = toRemoteTableName(connection, remoteSchema, tableName);
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (Column column : relation.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnString(column));
            }
            String sql = format(
                    "CREATE TABLE %s (%s)",
                    quoted(catalog, remoteSchema, tableName),
                    join(", ", columnList.build()));
            log.debug(String.format("sql: %s", sql));
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    connectorId,
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    tableName);
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Nullable
    @Override
    public JdbcTableHandle getTableHandle(SchemaTableName schemaTableName)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            String remoteSchema = toRemoteSchemaName(connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(connection, remoteSchema, schemaTableName.getTableName());
            try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.of(remoteTable))) {
                List<JdbcTableHandle> tableHandles = new ArrayList<>();
                while (resultSet.next()) {
                    tableHandles.add(new JdbcTableHandle(
                            connectorId,
                            schemaTableName,
                            resultSet.getString("TABLE_CAT"),
                            resultSet.getString("TABLE_SCHEM"),
                            resultSet.getString("TABLE_NAME")));
                }
                if (tableHandles.isEmpty()) {
                    return null;
                }
                if (tableHandles.size() > 1) {
                    throw new TreeTrackerException(NOT_SUPPORTED, "Multiple tables matched: " + schemaTableName);
                }
                return getOnlyElement(tableHandles);
            }
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public List<JdbcColumnHandle> getColumns(JdbcTableHandle tableHandle)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                            resultSet.getInt("DATA_TYPE"),
                            resultSet.getInt("COLUMN_SIZE"),
                            resultSet.getInt("DECIMAL_DIGITS"));
                    Optional<ReadMapping> columnMapping = toTreeTrackerType(typeHandle);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        boolean nullable = columnNullable == resultSet.getInt("NULLABLE");
                        columns.add(new JdbcColumnHandle(connectorId, columnName, typeHandle, columnMapping.get().getType(), nullable));
                    }
                }
                if (columns.isEmpty()) {
                    // In rare cases (e.g. PostgreSQL) a table might have no columns.
                    throw new TableNotFoundException(tableHandle.getSchemaTableName());
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<ReadMapping> toTreeTrackerType(JdbcTypeHandle typeHandle)
    {
        return StandardReadMappings.jdbcTypeToTreeTrackerType(typeHandle);
    }

    @Override
    public PreparedStatement buildSql(Connection connection, List<JdbcColumnHandle> columnHandles, JdbcTableHandle jdbcTableHandle)
            throws SQLException
    {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                connection,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                columnHandles);
    }

    @Override
    public int getTableSize(Connection connection, JdbcTableHandle jdbcTableHandle)
    {
        try (PreparedStatement statement1 = new QueryBuilder(identifierQuote).getTableSize(
                this,
                connection,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName())) {
            try (ResultSet rs = statement1.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return connection.prepareStatement(sql);
    }

    @Override
    public List<SchemaTableName> getTableNames(Optional<String> schema)
    {
        try (Connection connection = connectionFactory.openConnection()) {
            Optional<String> remoteSchema = schema.map(schemaName -> toRemoteSchemaName(connection, schemaName));
            try (ResultSet resultSet = getTables(connection, remoteSchema, Optional.empty())) {
                ImmutableList.Builder<SchemaTableName> list = ImmutableList.builder();
                while (resultSet.next()) {
                    String tableSchema = getTableSchemaName(resultSet);
                    String tableName = resultSet.getString("TABLE_NAME");
                    list.add(new SchemaTableName(tableSchema.toLowerCase(ENGLISH), tableName.toLowerCase(ENGLISH)));
                }
                return list.build();
            }
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public void ingestRelation(
            String schemaName,
            String relationName,
            List<String> attributes,
            List<Type> types,
            List<List<RelationalValue>> vals)
    {
        checkArgument(attributes.size() == types.size(), "attributes and types size not equal");
        if (!vals.isEmpty()) {
            checkArgument(attributes.size() == vals.get(0).size(), "attributes and one of the rows in vals not equal");
            RowSet rowSet = new RowSet();
            for (List<RelationalValue> val : vals) {
                rowSet.add(new ObjectRow(attributes, types, val));
            }
            Relation relation = new Relation(rowSet, relationName);
            createTable(schemaName, relation);
            insertTable(schemaName, relation);
        }
        else {
            List<Column> columns = new ArrayList<>();
            for (int i = 0; i < attributes.size(); ++i) {
                String attribute = attributes.get(i);
                Type type = types.get(i);
                columns.add(new Column(attribute, type, List.of()));
            }
            Relation relation = new Relation(relationName, columns);
            createTable(schemaName, relation);
        }
    }

    @Override
    public List<String> getAttributes(SchemaTableName schemaTableName)
    {
        JdbcTableHandle jdbcTableHandle = requireNonNull(getTableHandle(schemaTableName));
        List<JdbcColumnHandle> columnHandles = getColumns(jdbcTableHandle);
        return columnHandles.stream().map(JdbcColumnHandle::getColumnName).collect(Collectors.toList());
    }

    @Override
    public void createSchema(String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public float fractionOfUniqueValuesInColumn(JdbcTableHandle jdbcTableHandle, List<JdbcColumnHandle> columnHandles)
    {
        try (Connection connection = getConnection(); PreparedStatement statement1 = new QueryBuilder(identifierQuote).getNumUniqueForColumn(
                this,
                connection,
                jdbcTableHandle.getCatalogName(),
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                columnHandles)) {
            try (ResultSet rs = statement1.executeQuery()) {
                rs.next();
                int numberOfUniqueValues = rs.getInt(1);
                int tableSize = getTableSize(connection, jdbcTableHandle);
                return numberOfUniqueValues / (float) tableSize;
            }
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    protected String getTableSchemaName(ResultSet resultSet)
            throws SQLException
    {
        return resultSet.getString("TABLE_SCHEM");
    }

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException
    {
        requireNonNull(tableHandle);
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getColumns(
                tableHandle.getCatalogName(),
                escapeNamePattern(Optional.ofNullable(tableHandle.getSchemaName()), escape).orElse(null),
                escapeNamePattern(Optional.ofNullable(tableHandle.getTableName()), escape).orElse(null),
                null);
    }

    protected String toRemoteTableName(Connection connection, String remoteSchema, String tableName)
    {
        requireNonNull(remoteSchema, "remoteSchema is null");
        requireNonNull(tableName, "tableName is null");

        if (caseInsensitiveNameMatching) {
            try {
                Map<String, String> mapping = listTablesByLowerCase(connection, remoteSchema);
                String remoteTable = mapping.get(tableName);
                if (remoteTable != null) {
                    return remoteTable;
                }
            }
            catch (RuntimeException e) {
                throw new TreeTrackerException(JDBC_ERROR, "Failed to find remote table name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                return tableName.toUpperCase(ENGLISH);
            }
            return tableName;
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listTablesByLowerCase(Connection connection, String remoteSchema)
    {
        try (ResultSet resultSet = getTables(connection, Optional.of(remoteSchema), Optional.empty())) {
            ImmutableMap.Builder<String, String> map = ImmutableMap.builder();
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                map.put(tableName.toLowerCase(ENGLISH), tableName);
            }
            return map.build();
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }

    protected static Optional<String> escapeNamePattern(Optional<String> name, Optional<String> escape)
    {
        if (!name.isPresent() || !escape.isPresent()) {
            return name;
        }
        return Optional.of(escapeNamePattern(name.get(), escape.get()));
    }

    private static String escapeNamePattern(String name, String escape)
    {
        requireNonNull(name, "name is null");
        requireNonNull(escape, "escape is null");
        checkArgument(!escape.equals("_"), "Escape string must not be '_'");
        checkArgument(!escape.equals("%"), "Escape string must not be '%'");
        name = name.replace(escape, escape + escape);
        name = name.replace("_", escape + "_");
        name = name.replace("%", escape + "%");
        return name;
    }

    protected String toRemoteSchemaName(Connection connection, String schemaName)
    {
        requireNonNull(schemaName, "schemaName is null");
        verify(CharMatcher.forPredicate(Character::isUpperCase).matchesNoneOf(schemaName),
                "Expected schema name from internal metadata to be lowercase: %s", schemaName);

        if (caseInsensitiveNameMatching) {
            try {
                Map<String, String> mapping = listSchemasByLowerCase(connection);
                String remoteSchema = mapping.get(schemaName);
                if (remoteSchema != null) {
                    return remoteSchema;
                }
            }
            catch (RuntimeException e) {
                throw new TreeTrackerException(JDBC_ERROR, "Failed to find remote schema name: " + firstNonNull(e.getMessage(), e), e);
            }
        }

        try {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                return schemaName.toUpperCase(ENGLISH);
            }
            return schemaName;
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    protected Map<String, String> listSchemasByLowerCase(Connection connection)
    {
        return listSchemas(connection).stream()
                .collect(toImmutableMap(schemaName -> schemaName.toLowerCase(ENGLISH), schemaName -> schemaName));
    }

    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!schemaName.equalsIgnoreCase("information_schema")) {
                    schemaNames.add(schemaName);
                }
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    protected void execute(Connection connection, String query)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            log.debug(String.format("Execute: %s", query));
            statement.execute(query);
        }
    }

    protected String toSqlType(Type type)
    {
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            if (varcharType.isUnbounded()) {
                return "varchar";
            }
            return "varchar(" + varcharType.getLengthSafe() + ")";
        }
        if (type instanceof CharType) {
            if (type.getLength() == CharType.MAX_LENGTH) {
                return "char";
            }
            return "char(" + type.getLength() + ")";
        }
        String sqlType = SQL_TYPES.get(type);
        if (sqlType != null) {
            return sqlType;
        }
        throw new TreeTrackerException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private String getColumnString(Column column)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(column.getName()))
                .append(" ")
                .append(toSqlType(column.getType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    protected String quoted(String name)
    {
        name = name.replace(identifierQuote, identifierQuote + identifierQuote);
        return identifierQuote + name + identifierQuote;
    }

    protected String quoted(String catalog, String schema, String table)
    {
        StringBuilder sb = new StringBuilder();
        if (!isNullOrEmpty(catalog)) {
            sb.append(quoted(catalog)).append(".");
        }
        if (!isNullOrEmpty(schema)) {
            sb.append(quoted(schema)).append(".");
        }
        sb.append(quoted(table));
        return sb.toString();
    }

    private String buildInsertSql(List<Row> rows, String catalog, String remoteSchema, String tableName)
    {
        ImmutableList.Builder<String> vals = ImmutableList.builder();
        for (Row row : rows) {
            ImmutableList.Builder<String> valList = ImmutableList.builder();
            for (RelationalValue val : row.getVals()) {
                if (val == null) {
                    valList.add(String.format("%s", valRep(val, null)));
                }
                else {
                    valList.add(String.format("%s", valRep(val, val.getType())));
                }
            }
            vals.add("(" + Joiner.on(',').join(valList.build()) + ")");
        }
        return "INSERT INTO " +
                quoted(catalog, remoteSchema, tableName) +
                " VALUES " +
                Joiner.on(',').join(vals.build());
    }

    /**
     * Create val representation based on the column type. This is used, for instance, when generate the CREATE TABLE
     * statement.
     *
     * @param val Value object
     * @param columnType the type of column which val resides
     * @return the string representation of Val that can be used inside the SQL statement to JDBC
     */
    private String valRep(RelationalValue val, Type columnType)
    {
        if (val == null) {
            return "'NULL'";
        }
        if (columnType instanceof CharType || columnType instanceof VarcharType) {
            return "'" + val + "'";
        }
        if (columnType instanceof IntegerType) {
            return val + "";
        }
        throw new IllegalArgumentException("Unsupported column type: " + columnType.getDisplayName());
    }

    public String getIdentifierQuote()
    {
        return identifierQuote;
    }
}
