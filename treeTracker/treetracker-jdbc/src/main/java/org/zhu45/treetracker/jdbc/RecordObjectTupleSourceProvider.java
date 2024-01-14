package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.TableHandle;
import org.zhu45.treetracker.common.row.ObjectRow;

import java.util.List;

public class RecordObjectTupleSourceProvider
        extends AbstractRecordTupleSourceProvider<ObjectRow>
{
    public RecordObjectTupleSourceProvider(RecordSetProvider recordSetProvider)
    {
        super(recordSetProvider);
    }

    public ObjectRecordTupleSource createTupleSource(TableHandle tableHandle, List<? extends ColumnHandle> columns)
    {
        return new ObjectRecordTupleSource(recordSetProvider.getRecordSet(tableHandle, columns));
    }
}
