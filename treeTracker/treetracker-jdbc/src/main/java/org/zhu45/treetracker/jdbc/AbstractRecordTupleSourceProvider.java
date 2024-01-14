package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.row.Row;

import static java.util.Objects.requireNonNull;

public abstract class AbstractRecordTupleSourceProvider<R extends Row>
        implements RecordTupleSourceProvider<R>
{
    protected final RecordSetProvider recordSetProvider;

    public AbstractRecordTupleSourceProvider(RecordSetProvider recordSetProvider)
    {
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }
}
