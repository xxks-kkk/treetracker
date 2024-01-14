package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.TableHandle;

import java.util.List;

public interface RecordSetProvider
{
    RecordSet getRecordSet(TableHandle tableHandle, List<? extends ColumnHandle> columns);
}
