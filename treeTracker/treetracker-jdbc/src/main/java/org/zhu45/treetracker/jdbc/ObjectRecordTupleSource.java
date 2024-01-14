package org.zhu45.treetracker.jdbc;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.type.Type;

import java.util.List;
import java.util.NoSuchElementException;

import static java.lang.String.format;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.zhu45.treetracker.common.StandardErrorCode.NOT_SUPPORTED;

public class ObjectRecordTupleSource
        extends BaseRecordTupleSource<ObjectRow>
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(ObjectRecordTupleSource.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    public ObjectRecordTupleSource(RecordSet recordSet)
    {
        super(recordSet);
    }

    public ObjectRecordTupleSource(List<Type> types, List<String> attributes, RecordCursor cursor)
    {
        super(types, attributes, cursor);
    }

    public ObjectRow getNextRow()
    {
        List<RelationalValue> vals = FastList.newList(cursor.numColumns());
        if (!cursor.advanceNextPosition()) {
            closed = true;
            throw new NoSuchElementException();
        }
        if (!closed) {
            for (int column = 0; column < types.size(); column++) {
                // WARNING: For performance reason, we don't check for null, i.e., we don't support null.
                // If there is a null in tuple, the behavior of the program is undefined.
                Type type = types.get(column);
                Class<?> javaType = type.getJavaType();
                if (javaType == String.class) {
                    vals.add(new StringValue(cursor.getType(column), cursor.getString(column)));
                }
                else if (javaType == long.class) {
                    long val = cursor.getLong(column);
                    if (val < Integer.MAX_VALUE && val > Integer.MIN_VALUE) {
                        vals.add(new IntegerValue((int) val));
                    }
                    else {
                        throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, format("Value %d is out of range integer", Long.valueOf(val)));
                    }
                }
                else {
                    throw new TreeTrackerException(NOT_SUPPORTED, "unsupported type: " + type);
                }
            }
        }
        return new ObjectRow(attributes, types, vals);
    }
}
