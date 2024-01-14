package org.zhu45.treetracker.jdbc;

import com.google.common.base.Joiner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.Row;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;

public class PostgreSqlClient
        extends BaseJdbcClient
{
    private static final Logger log;

    static {
        log = LogManager.getLogger(PostgreSqlClient.class);
    }

    public PostgreSqlClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(new JdbcConnectorId("postgres"), config, "\"", connectionFactory);
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(
                connection.getCatalog(),
                escapeNamePattern(schemaName, escape).orElse(null),
                escapeNamePattern(tableName, escape).orElse(null),
                new String[] {"TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"});
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        connection.setAutoCommit(false);
        PreparedStatement statement = connection.prepareStatement(sql,
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(1000);
        return statement;
    }

    @Override
    public void loadTable(SchemaTableName schemaTableName, String filePath, String delimiter)
    {
        if (getTableHandle(schemaTableName) == null) {
            throw new TreeTrackerException(JDBC_ERROR, schemaTableName + "is not found");
        }
        File f = new File(filePath);
        if (f.isFile() && !f.isDirectory()) {
            try (Connection connection = connectionFactory.openConnection()) {
                String remoteSchema = toRemoteSchemaName(connection, schemaTableName.getSchemaName());
                String copyStatement = format(
                        "COPY %s.%s FROM '%s' WITH DELIMITER '%s' NULL '\\N'  CSV",
                        remoteSchema, quoted(schemaTableName.getTableName()), f.getAbsolutePath(), delimiter);
                log.debug(String.format("copyStatement: %s", copyStatement));
                execute(connection, copyStatement);
            }
            catch (SQLException e) {
                throw new TreeTrackerException(JDBC_ERROR, e);
            }
        }
        else {
            throw new TreeTrackerException(JDBC_ERROR, "No such file on " + filePath);
        }
    }

    @Override
    public void createSchema(String schemaName)
    {
        try (Connection connection = connectionFactory.openConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    @Override
    public void insertTable(String schemaName, Relation relation)
    {
        if (numberOfValsSmallerThanMaximum(relation)) {
            insertTableJdbc(schemaName, relation);
        }
        else {
            String tableName = relation.getRelationName();
            try (Connection connection = connectionFactory.openConnection()) {
                boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
                String remoteSchema = toRemoteSchemaName(connection, schemaName);
                if (uppercase) {
                    tableName = tableName.toUpperCase(ENGLISH);
                }
                String catalog = connection.getCatalog();
                Pair<Path, Path> pair = insertUsingCopy(relation.getRows(), catalog, remoteSchema, tableName);
                Path csvFilePath = pair.getKey();
                Path sqlScriptPath = pair.getValue();
                ScriptRunner sr = new ScriptRunner(connection);
                Reader reader = new BufferedReader(new FileReader(sqlScriptPath.toFile()));
                sr.runScript(reader);
                boolean isDeleted = Files.deleteIfExists(csvFilePath);
                checkArgument(isDeleted, String.format("csvFilePath: %s is not deleted", csvFilePath));
                isDeleted = Files.deleteIfExists(sqlScriptPath);
                checkArgument(isDeleted, String.format("sqlScriptPath: %s is not deleted", sqlScriptPath));
            }
            catch (SQLException e) {
                throw new TreeTrackerException(JDBC_ERROR, e);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Create a CSV file that contains data, and generate the sql script about loading the data. Returns the path to the CSV.
     */
    private Pair<Path, Path> insertUsingCopy(List<Row> rows, String catalog, String remoteSchema, String tableName)
    {
        String csvFileName = Joiner.on('.').join(List.of(catalog, remoteSchema, tableName, "csv"));
        String sqlScriptFileName = Joiner.on('.').join(List.of(catalog, remoteSchema, tableName, "sql"));

        CSVFormat format = CSVFormat.DEFAULT.builder().build();
        try (Writer writer = new FileWriter(csvFileName);
                CSVPrinter csvPrinter = new CSVPrinter(writer, format)) {
            for (Row row : rows) {
                csvPrinter.printRecord(row.getVals());
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (Writer writer = new FileWriter(sqlScriptFileName)) {
            writer.write("BEGIN TRANSACTION;\n");
            String copyStatement = new StringBuilder()
                    .append("COPY ")
                    .append(quoted(catalog, remoteSchema, tableName))
                    .append(" FROM ")
                    .append("'")
                    .append(Paths.get(csvFileName).toAbsolutePath())
                    .append("'")
                    .append(" WITH DELIMITER AS ',' NULL AS '' QUOTE AS '\"' ESCAPE AS '\\' CSV;\n")
                    .toString();
            writer.write(copyStatement);
            writer.write("END TRANSACTION;");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return Pair.of(Paths.get(csvFileName), Paths.get(sqlScriptFileName));
    }
}
