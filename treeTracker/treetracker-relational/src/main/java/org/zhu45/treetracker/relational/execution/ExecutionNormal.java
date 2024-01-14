package org.zhu45.treetracker.relational.execution;

import de.renebergelt.test.Switches;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.PlanNode;

/**
 * This represents a typical demand-driven evaluation of physical plan.
 */
public class ExecutionNormal
        extends ExecutionBase
{
    private final Operator rootOperator;
    private final Logger traceLogger = LogManager.getLogger(ExecutionNormal.class.getName());

    /**
     * Execute a physical plan
     *
     * @param root the root node of a physical plan
     */
    public ExecutionNormal(PlanNode root)
    {
        super(root);
        this.rootOperator = root.getOperator();
    }

    @Override
    public void open()
    {
        rootOperator.open();
    }

    @Override
    public Row getNext()
    {
        return rootOperator.getNext();
    }

    @Override
    public void close()
    {
        rootOperator.close();
    }

    @Override
    public MultiSet<Row> eval()
    {
        int i = 0;
        open();
        MultiSet<Row> resultSet = new HashMultiSet<>();
        while (true) {
            if (traceLogger.isTraceEnabled()) {
                i++;
                traceLogger.trace(String.format("\n%s GetNext() call", i));
            }
            Row row = getNext();
            if (row == null) {
                close();
                return resultSet;
            }
            resultSet.add(row);
        }
    }

    /**
     * Use this during benchmark because when the data is large, majority of
     * computation is spent on adding result to the resultSet. Thus, for benchmark,
     * we don't keep track of resultSet.
     */
    public void evalForBenchmark()
    {
        int i = 0;
        open();
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace("\n");
        }
        while (true) {
            Row row = getNext();
            i++;
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                if (i % 3000 == 0) {
                    System.out.println("number of join result computed so far: " + i);
                }
            }
            if (row == null) {
                close();
                break;
            }
        }
    }

    /**
     * Similar to evalForBenchmark but the function also returns the result set size.
     */
    public long evalForBenchmarkWithResultSize()
    {
        long i = 0;
        open();
        while (true) {
            Row row = getNext();
            if (row == null) {
                close();
                break;
            }
            else {
                i++;
            }
        }
        return i;
    }
}
