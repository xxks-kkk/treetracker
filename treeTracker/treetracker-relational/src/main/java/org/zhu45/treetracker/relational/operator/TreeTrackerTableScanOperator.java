package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.ObjectRow;

import static java.util.Objects.requireNonNull;

public class TreeTrackerTableScanOperator
        extends TupleBasedHighPerfTableScanOperator
{
    private Logger traceLogger;

    public TreeTrackerTableScanOperator()
    {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TreeTrackerTableScanOperator.class);
        }
    }

    @Override
    public ObjectRow passContext(OperatorInformation info)
    {
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
        }
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
            incrementTraceDepth();
        }
        noGoodListMap.updateNoGoodListMap(pVal, info);
        return null;
    }
}
