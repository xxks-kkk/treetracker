package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.row.Row;

public interface RecordTupleSource<R extends Row>
{
    void reset();
    R getNextRow();
    boolean hasNext();
    void close();
}
