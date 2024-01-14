package org.zhu45.treetracker.jdbc;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.type.Type;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class IntRecordTupleSource
        extends BaseRecordTupleSource<IntRow>
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(IntRecordTupleSource.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    public IntRecordTupleSource(RecordSet recordSet)
    {
        super(recordSet);
    }

    public IntRow getNextRow()
    {
        int[] vals = new int[numColumns];

        if (!cursor.advanceNextPosition()) {
            closed = true;
            throw new NoSuchElementException();
        }
        if (!closed) {
            for (int column = 0; column < types.size(); column++) {
                Type type = types.get(column);
                Class<?> javaType = type.getJavaType();
                if (Switches.DEBUG) {
                    checkState(javaType == long.class);
                }
                long val = cursor.getLong(column);
                if (val < Integer.MAX_VALUE && val > Integer.MIN_VALUE) {
                    vals[column] = (int) val;
                }
                else {
                    throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, format("Value %d is out of range integer", Long.valueOf(val)));
                }
            }
        }
        return new IntRow(attributes, vals);
    }
}
