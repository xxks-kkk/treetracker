package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.TableHandle;
import org.zhu45.treetracker.common.row.Row;

import java.util.List;

public interface RecordTupleSourceProvider<R extends Row>
{
    RecordTupleSource<R> createTupleSource(TableHandle tableHandle, List<? extends ColumnHandle> columns);
}
