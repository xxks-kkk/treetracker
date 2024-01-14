package org.zhu45.treetracker.jdbc;

import com.google.common.collect.ImmutableList;
import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.TableHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class JdbcRecordSetProvider
        implements RecordSetProvider
{
    private final JdbcClient jdbcClient;

    public JdbcRecordSetProvider(JdbcClient jdbcClient)
    {
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public RecordSet getRecordSet(TableHandle tableHandle, List<? extends ColumnHandle> columns)
    {
        ImmutableList.Builder<JdbcColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((JdbcColumnHandle) handle);
        }

        return new JdbcRecordSet(jdbcClient, (JdbcTableHandle) tableHandle, handles.build());
    }
}
