package org.zhu45.treetracker.jdbc;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.Utils.appendCallerInfo;

public abstract class BaseRecordTupleSource<R extends Row>
        implements RecordTupleSource<R>
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(BaseRecordTupleSource.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    protected RecordCursor cursor;
    protected List<String> attributes;
    protected List<Type> types;
    protected boolean closed;
    protected int numColumns;

    public BaseRecordTupleSource(RecordSet recordSet)
    {
        this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(),
                recordSet.getColumnNames(),
                recordSet.cursor());
        if (Switches.DEBUG) {
            if (traceLogger.isDebugEnabled()) {
                traceLogger.debug(appendCallerInfo("called recordSet.cursor()", 4));
            }
        }
    }

    public BaseRecordTupleSource(List<Type> types, List<String> attributes, RecordCursor cursor)
    {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.types = requireNonNull(types, "types is null");
        this.attributes = requireNonNull(attributes, "attributes is null");
        this.numColumns = types.size();
    }

    public void reset()
    {
        closed = false;
        cursor.reset();
    }

    public abstract R getNextRow();

    public boolean hasNext()
    {
        return cursor.hasNext();
    }

    public void close()
    {
        closed = true;
        cursor.close();
        attributes = null;
        types = null;
    }
}
