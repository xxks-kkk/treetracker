package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;

import static java.util.Objects.requireNonNull;

public class TreeTrackerTableScanV2Operator
        extends TupleBasedHighPerfTableScanOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TreeTrackerTableScanV2Operator.class);
        }
        else {
            traceLogger = null;
        }
    }

    public TreeTrackerTableScanV2Operator()
    {
    }

    @Override
    public Row passContext(OperatorInformation info)
    {
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
                incrementTraceDepth();
            }
        }
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        noGoodListMap.updateNoGoodListMap(pVal, info);
        Row tmp = getNext();
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return: " + tmp));
            decrementTraceDepth();
        }
        return tmp;
    }
}
