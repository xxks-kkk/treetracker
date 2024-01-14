package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.type.Type;

import java.util.List;

public interface RecordSet
{
    List<Type> getColumnTypes();

    List<String> getColumnNames();

    RecordCursor cursor();
}
