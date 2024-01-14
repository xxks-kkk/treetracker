package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodListMap;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TupleBaseTreeTrackerOneBetaTableScanOperator
        extends TupleBasedTableScanOperator
{
    private Logger traceLogger;
    private NoGoodListMap noGoodListMap;

    public TupleBaseTreeTrackerOneBetaTableScanOperator()
    {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedTableScanOperator.class.getName());
        }
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedTableScanStatisticsInformation();
        }
    }

    @Override
    public void close()
    {
        if (Switches.STATS) {
            noGoodListMap.updateStatisticsInformation();
            statisticsInformation.setRecordTupleSourceClazzName(recordTupleSourceProvider.getClass().getCanonicalName());
        }
        finish();
    }

    @Override
    public Row passContext(OperatorInformation info)
    {
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
            incrementTraceDepth();
        }
        TupleBaseTreeTrackerOneBetaHashTableOperator.Context context = (TupleBaseTreeTrackerOneBetaHashTableOperator.Context) info;
        if (context.getGetNewR1()) {
            pVal.setIsGood(true);
            return null;
        }
        else if (!pVal.getIsGood()) {
            noGoodListMap.updateNoGoodListMap(pVal, null);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("noGoodListMap: " + noGoodListMap));
            }
        }
        Row tmp = getNext();
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return: " + tmp));
            decrementTraceDepth();
        }
        return tmp;
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }
        if (Switches.DEBUG) {
            checkArgument(source != null || useDomainAsSource,
                    "ObjectRecordTupleSource cannot be null or the tuples have been populated in the table scan operator memory");
        }
        try {
            while (true) {
                Row tmp = getNextRow();
                if (noGoodListMap.isGood(tmp)) {
                    pVal = tmp;
                    break;
                }
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("pVal: " + pVal));
                traceLogger.trace(formatTraceMessage("return: " + pVal));
                decrementTraceDepth();
            }
            if (Switches.STATS) {
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            return pVal;
        }
        catch (NoSuchElementException e) {
            pVal = null;
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("pVal: " + pVal));
                traceLogger.trace(formatTraceMessage("return: " + pVal));
                decrementTraceDepth();
            }
            if (Switches.STATS) {
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            return pVal;
        }
    }

    @Override
    public void setNoGoodListMap(NoGoodListMap noGoodListMap)
    {
        this.noGoodListMap = noGoodListMap;
    }

    @Override
    public NoGoodListMap getNoGoodListMap()
    {
        return this.noGoodListMap;
    }
}
