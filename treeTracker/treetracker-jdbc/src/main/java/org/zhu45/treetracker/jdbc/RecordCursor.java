package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.type.Type;

import java.io.Closeable;

public interface RecordCursor
        extends Closeable
{
    boolean advanceNextPosition();

    void reset();

    String getString(int field);

    long getLong(int field);

    Type getType(int field);

    boolean isNull(int field);

    boolean hasNext();

    int numColumns();

    @Override
    void close();
}
