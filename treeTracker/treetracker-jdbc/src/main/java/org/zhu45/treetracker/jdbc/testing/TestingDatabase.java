package org.zhu45.treetracker.jdbc.testing;

import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcColumnHandle;
import org.zhu45.treetracker.jdbc.JdbcRecordSetProvider;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.ObjectRecordTupleSource;
import org.zhu45.treetracker.jdbc.RecordObjectTupleSourceProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_NOT_IMPLEMENTED;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;

public class TestingDatabase
        implements Database<ObjectRow>
{
    private final JdbcClient jdbcClient;

    public TestingDatabase()
    {
        jdbcClient = postgresJdbcClientSupplier.get();
    }

    public TestingDatabase(JdbcClient jdbcClient)
    {
        this.jdbcClient = jdbcClient;
    }

    @Override
    public String getSchemaName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    @Override
    public List<SchemaTableName> getRelations()
    {
        throw new TreeTrackerException(FUNCTION_NOT_IMPLEMENTED, "getRelations should be implemented by the subclass");
    }

    @Override
    public Optional<Long> getSeed()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowSet<ObjectRow> getRelationRows(String relationName)
    {
        SchemaTableName schemaTableName = new SchemaTableName(getSchemaName(), relationName);
        JdbcTableHandle tableHandle = jdbcClient.getTableHandle(schemaTableName);
        RecordObjectTupleSourceProvider recordTupleSourceProvider =
                new RecordObjectTupleSourceProvider(new JdbcRecordSetProvider(jdbcClient));
        List<JdbcColumnHandle> jdbcColumnHandleList =
                jdbcClient.getColumns(tableHandle);
        ObjectRecordTupleSource recordTupleSource = recordTupleSourceProvider.createTupleSource(tableHandle, jdbcColumnHandleList);
        MultiSet<ObjectRow> res = new HashMultiSet<>();
        while (recordTupleSource.hasNext()) {
            res.add(recordTupleSource.getNextRow());
        }
        recordTupleSource.close();
        List<ObjectRow> rows = new ArrayList<>(res);
        return new RowSet<>(rows);
    }

    @Override
    public int getNumRelations()
    {
        throw new UnsupportedOperationException("TestingDatabase doesn't support this operation");
    }
}
