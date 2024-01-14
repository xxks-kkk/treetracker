package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.type.Type;

public interface ColumnHandle
{
    String getColumnName();

    Type getColumnType();
}
