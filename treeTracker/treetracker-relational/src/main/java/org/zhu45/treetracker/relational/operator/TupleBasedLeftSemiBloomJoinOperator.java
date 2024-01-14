package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.BlockedBloom;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

public class TupleBasedLeftSemiBloomJoinOperator
        extends TupleBasedJoinOperator
{
    private static Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedLeftSemiBloomJoinOperator.class.getName());
        }
    }

    BlockedBloom blockedBloom;

    public TupleBasedLeftSemiBloomJoinOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedJoinStatisticsInformation();
        }
    }

    @Override
    public void open()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".open()"));
            incrementTraceDepth();
        }
        r1Operator.open();
        r2Operator.open();

        blockedBloom = new BlockedBloom((int) operatorAssociatedRelationSize, 32);

        while (true) {
            r2 = r2Operator.getNext();
            if (r2 == null) {
                r2Operator.close();
                break;
            }
            JoinValueContainerKey jav = extract(r2, false);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("jav: " + jav));
            }
            blockedBloom.add(jav.hashCode());
        }
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }

        while (true) {
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                statisticsInformation.incrementNumberOfR1Assignments();
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("(%s) numberOfR1Assignments after increment: %s",
                            getOperatorID(),
                            statisticsInformation.getNumberOfR1Assignments())));
                }
            }
            if (r1 == null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + null));
                    decrementTraceDepth();
                }
                return null;
            }
            JoinValueContainerKey jav = extract(r1, true);
            if (blockedBloom.mayContain(jav.hashCode())) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + r1));
                    decrementTraceDepth();
                }
                return r1;
            }
        }
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedLeftSemiBloomJoinOperator(this, context);
    }
}
