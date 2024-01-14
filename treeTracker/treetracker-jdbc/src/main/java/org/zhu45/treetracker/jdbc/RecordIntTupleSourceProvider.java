package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.TableHandle;
import org.zhu45.treetracker.common.row.IntRow;

import java.util.List;

public class RecordIntTupleSourceProvider
        extends AbstractRecordTupleSourceProvider<IntRow>
{
    public RecordIntTupleSourceProvider(RecordSetProvider recordSetProvider)
    {
        super(recordSetProvider);
    }

    public IntRecordTupleSource createTupleSource(TableHandle tableHandle, List<? extends ColumnHandle> columns)
    {
        return new IntRecordTupleSource(recordSetProvider.getRecordSet(tableHandle, columns));
    }
}
