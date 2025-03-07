package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.RecordTupleSource;
import org.zhu45.treetracker.jdbc.RecordTupleSourceProvider;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodListMap;
import org.zhu45.treetracker.relational.planner.catalog.TableCatalog;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TupleBasedTableScanOperator
        extends AbstractOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedTableScanOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    protected RecordTupleSourceProvider recordTupleSourceProvider;
    private static final OptType operatorType = OptType.table;
    protected RecordTupleSource source;
    private boolean finished;
    private MultiwayJoinNode multiwayJoinNode;
    protected Row pVal;
    protected boolean useDomainAsSource;
    private Iterator<Row> domainIterator;
    protected SchemaTableName schemaTableName;
    protected TableCatalog tableCatalog;

    protected long fetchingTuplesTime;
    protected long fetchingTupleTimeMarker;
    protected long predicateEvaluationTime;
    private long predicateEvaluationTimeMarker;
    protected boolean statisticsUpdatedInClose;

    public TupleBasedTableScanOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedTableScanStatisticsInformation();
        }
    }

    @Override
    public void close()
    {
        if (Switches.STATS) {
            statisticsInformation.setFetchingTuplesTime(fetchingTuplesTime);
            if (useDomainAsSource) {
                statisticsInformation.setNumberOfTuples(multiwayJoinNode.getDomain().size());
            }
            else {
                statisticsInformation.setNumberOfTuples(operatorAssociatedRelationSize);
            }
            statisticsInformation.setRecordTupleSourceClazzName(recordTupleSourceProvider.getClass().getCanonicalName());
            statisticsInformation.setPredicateEvaluationTime(predicateEvaluationTime);
        }
        finish();
    }

    @Override
    public void setChildren(List<Operator> children)
    {
        //noop
    }

    @Override
    public void reset()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("reset is called"));
            traceLogger.trace(formatTraceMessage("useDomainAsSource: " + useDomainAsSource));
        }
        if (!useDomainAsSource) {
            if (source == null) {
                if (Switches.STATS) {
                    predicateEvaluationTimeMarker = System.nanoTime();
                }
                source = recordTupleSourceProvider.createTupleSource(tableCatalog.getTableHandle(), tableCatalog.getColumnHandles());
                if (Switches.STATS) {
                    predicateEvaluationTime += (System.nanoTime() - predicateEvaluationTimeMarker);
                }
            }
            source.reset();
        }
        else {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("domain size: " + multiwayJoinNode.getDomain().size()));
            }
            useDomainAsSource = true;
            initalizeDomainIterator();
        }
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedTableScanOperator(this, context);
    }

    public void finish()
    {
        finished = true;

        if (source != null) {
            source.close();
        }

        source = null;
    }

    @Override
    public void open()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".open()"));
        }
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedTableScanStatisticsInformation();
            fetchingTuplesTime = 0;
            fetchingTupleTimeMarker = 0;
        }
        reset();
    }

    @Override
    public String getTraceOperatorName()
    {
        return schemaTableName.getTableName();
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG) {
            checkArgument(source != null || useDomainAsSource,
                    "ObjectRecordTupleSource cannot be null or the tuples have been populated in the table scan operator memory");
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
                incrementTraceDepth();
            }
        }
        try {
            if (Switches.STATS) {
                fetchingTupleTimeMarker = System.nanoTime();
            }
            pVal = getNextRow();
            if (Switches.STATS) {
                if (Switches.DEBUG) {
                    long fetchSingleRowTime = System.nanoTime() - fetchingTupleTimeMarker;
                    traceLogger.warn("fetchSingleRowTime (ns): " + fetchSingleRowTime);
                }
                fetchingTuplesTime += (System.nanoTime() - fetchingTupleTimeMarker);
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("pVal: " + pVal));
                traceLogger.trace(formatTraceMessage("return: " + pVal));
                decrementTraceDepth();
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

    protected Row getNextRow()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("getNextRow()"));
            incrementTraceDepth();
            traceLogger.trace(formatTraceMessage("useDomainAsSource: " + useDomainAsSource));
            if (multiwayJoinNode != null) {
                traceLogger.trace(formatTraceMessage("domain: " + multiwayJoinNode.getDomain()));
            }
            decrementTraceDepth();
        }
        if (useDomainAsSource) {
            return domainIterator.next();
        }
        else {
            return source.getNextRow();
        }
    }

    @Override
    public List<? extends ColumnHandle> getColumns()
    {
        return tableCatalog.getColumnHandles();
    }

    @Override
    public OptType getOperatorType()
    {
        return operatorType;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("id: ").append(getOperatorID()).append("\n");
        sb.append("name: ").append(getOperatorName()).append("\n");
        return sb.toString();
    }

    @Override
    public void setMultiwayJoinNode(MultiwayJoinNode node)
    {
        this.multiwayJoinNode = node;
    }

    @Override
    public MultiwayJoinNode getMultiwayJoinNode()
    {
        return this.multiwayJoinNode;
    }

    @Override
    public Row passContext(OperatorInformation info)
    {
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
                incrementTraceDepth();
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
    public IntRow passContext(int parentId, int id)
    {
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        if (Switches.DEBUG) {
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + parentId + ")"));
                incrementTraceDepth();
            }
        }
        Row tmp = getNext();
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return: " + tmp));
            decrementTraceDepth();
        }
        return (IntRow) tmp;
    }

    public void initalizeDomainIterator()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("initalizeDomainIterator()"));
        }
        MultiwayJoinDomain domain = multiwayJoinNode.getDomain();
        if (Switches.STATS) {
            if (domain.size() <= TupleBasedTableScanStatisticsInformation.maxDomainSize) {
                statisticsInformation.setDomain(multiwayJoinNode.getDomain());
            }
            statisticsInformation.setDomainSize(domain.size());
        }
        domainIterator = domain.iterator();
    }

    @Override
    public void setUseDomainAsSource(boolean useDomainAsSource)
    {
        this.useDomainAsSource = useDomainAsSource;
    }

    @Override
    public boolean getUseDomainAsSource()
    {
        return this.useDomainAsSource;
    }

    @Override
    public void setSchemaTableName(SchemaTableName schemaTableName)
    {
        this.schemaTableName = schemaTableName;
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public NoGoodListMap getNoGoodListMap()
    {
        return null;
    }

    @Override
    public void setNoGoodListMap(NoGoodListMap noGoodListMap)
    {
        // no-op
    }

    @Override
    public void setRecordTupleSourceProvider(RecordTupleSourceProvider recordTupleSourceProvider)
    {
        this.recordTupleSourceProvider = recordTupleSourceProvider;
    }

    @Override
    public void setTableCatalog(TableCatalog tableCatalog)
    {
        this.tableCatalog = requireNonNull(tableCatalog, "tableCatalog is null");
    }

    @Override
    public Class<? extends RecordTupleSourceProvider> getRecordTupleSourceProviderClazz()
    {
        return recordTupleSourceProvider.getClass();
    }

    @Override
    public MultiwayJoinNode getChildMultiwayJoinNode()
    {
        return null;
    }
}
