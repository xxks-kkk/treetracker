package org.zhu45.treektracker.multiwayJoin.testing;

import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.JdbcColumnHandle;
import org.zhu45.treetracker.jdbc.JdbcRecordSetProvider;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.ObjectRecordTupleSource;
import org.zhu45.treetracker.jdbc.RecordObjectTupleSourceProvider;
import org.zhu45.treetracker.jdbc.testing.Database;
import org.zhu45.treetracker.jdbc.testing.NaturalJoinJdbcClient;

import java.util.ArrayList;
import java.util.List;

public class PostgresNaturalJoinExecutor
{
    private final Database database;
    private final String naturalJoinTable;

    public PostgresNaturalJoinExecutor(Database database, String naturalJoinTableName)
    {
        this.database = database;
        this.naturalJoinTable = naturalJoinTableName;
    }

    /**
     * Execute a list of tables as a natural join query on the Postgres and get the result set.
     * We use this function to generate result set of multiway join in Postgres and compare the result with TT-J.
     * In addition, this function is used to verify the result of physical plan evaluation.
     *
     * @return the result of executing natural join query in Postgres
     */
    public MultiSet<Row> executeNaturalJoinOnPostgres(List<SchemaTableName> schemaTableNames)
    {
        NaturalJoinJdbcClient jdbcClient = (NaturalJoinJdbcClient) database.getJdbcClient();
        List<JdbcTableHandle> jdbcTableHandleList = new ArrayList<>();
        schemaTableNames.forEach(schemaTableName -> {
            jdbcTableHandleList.add(database.getJdbcClient().getTableHandle(schemaTableName));
        });
        JdbcTableHandle tableHandle = database.getJdbcClient().getTableHandle(new SchemaTableName(database.getSchemaName(), naturalJoinTable));
        if (tableHandle != null) {
            database.getJdbcClient().dropTable(tableHandle);
        }
        JdbcTableHandle jdbcTableHandle = jdbcClient.createNaturalJoinSqlTable(
                database.getSchemaName(),
                naturalJoinTable,
                jdbcTableHandleList);
        RecordObjectTupleSourceProvider recordTupleSourceProvider =
                new RecordObjectTupleSourceProvider(new JdbcRecordSetProvider(jdbcClient));
        List<JdbcColumnHandle> jdbcColumnHandleList =
                database.getJdbcClient().getColumns(jdbcTableHandle);
        ObjectRecordTupleSource recordTupleSource = recordTupleSourceProvider.createTupleSource(jdbcTableHandle, jdbcColumnHandleList);
        MultiSet<Row> res = new HashMultiSet<>();
        while (recordTupleSource.hasNext()) {
            res.add(recordTupleSource.getNextRow());
        }
        recordTupleSource.close();
        return res;
    }
}
