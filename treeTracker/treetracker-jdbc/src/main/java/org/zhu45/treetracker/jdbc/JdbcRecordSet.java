package org.zhu45.treetracker.jdbc;

import com.google.common.collect.ImmutableList;
import org.zhu45.treetracker.common.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JdbcRecordSet
        implements RecordSet
{
    private final JdbcClient jdbcClient;
    private final JdbcTableHandle jdbcTableHandle;
    private final List<JdbcColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final List<String> columnNames;

    public JdbcRecordSet(JdbcClient jdbcClient,
            JdbcTableHandle jdbcTableHandle,
            List<JdbcColumnHandle> columnHandles)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        this.jdbcTableHandle = requireNonNull(jdbcTableHandle, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        for (JdbcColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
            columnNames.add(column.getColumnName());
        }
        this.columnTypes = types.build();
        this.columnNames = columnNames.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public List<String> getColumnNames()
    {
        return columnNames;
    }

    @Override
    public RecordCursor cursor()
    {
        return new JdbcRecordCursor(jdbcClient, columnHandles, jdbcTableHandle);
    }
}
