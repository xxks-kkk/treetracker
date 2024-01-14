package org.zhu45.treetracker.relational.operator;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.relational.JoinValueContainerKey;
import org.zhu45.treetracker.relational.OptType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.inject.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.Utils.properPrintList;

public class TupleBasedLIPHashJoinOperator
        extends TupleBasedHashJoinOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedLIPHashJoinOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private final double bloomFilterFalsePositiveRate = 0.01;
    private BloomFilter<JoinValueContainerKey> bloomFilter;
    // The index of attributes of the fact table that can join with the base relation connected with
    // the current join operator. For example, the fact table R(a,b) and the base relation S(a,c,b).
    // Then, factTableJoinAttributeIdx = [0,1] because attribute a of R appears at pos 0 and
    // attribute b of R appears at pos 1, which both attributes also appear in S as well.
    private List<Integer> factTableJoinAttributeIdx;
    // <index of column in R1, index of column in R2> are potentially joinable.
    // This is only used to fast construct jav for the Bloom filter of base relation.
    private HashMap<Integer, Integer> joinIdxTmp;
    private List<String> baseRelationAttributes;

    private long buildBloomFiltersTimeMarker;
    private long buildBloomFiltersTime;

    public TupleBasedLIPHashJoinOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedLIPHashJoinStatisticsInformation();
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
        construct(JoinType.NaturalJoin);

        if (Switches.STATS) {
            buildBloomFiltersTimeMarker = System.nanoTime();
        }
        Funnel<JoinValueContainerKey> javFunnel = (joinValueContainerKey, primitiveSink) -> primitiveSink.putInt(joinValueContainerKey.hashCode());
        bloomFilter = BloomFilter.create(
                javFunnel,
                operatorAssociatedRelationSize,
                bloomFilterFalsePositiveRate);
        populateFactTableJoinAttributeIdx();
        if (Switches.STATS) {
            buildBloomFiltersTime += (System.nanoTime() - buildBloomFiltersTimeMarker);
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("baseRelationAttributes: " + properPrintList(baseRelationAttributes)));
            traceLogger.trace(formatTraceMessage("factTableRAttributes: " + properPrintList(planBuildContext
                    .getLeftMostPlanNodeOperator().getColumns().stream().map(ColumnHandle::getColumnName).collect(Collectors.toList()))));
            traceLogger.trace(formatTraceMessage("factTableJoinAttributeIdx: " + properPrintList(factTableJoinAttributeIdx)));
        }

        hashTableH = UnifiedMap.newMap();
        while (true) {
            r2 = r2Operator.getNext();
            if (r2 == null) {
                r2Operator.close();
                break;
            }
            if (Switches.STATS) {
                buildBloomFiltersTimeMarker = System.nanoTime();
            }
            JoinValueContainerKey javForBloomFilter = constructJavForBloomFilters(r2);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("javForBloomFilter: " + javForBloomFilter));
            }
            bloomFilter.put(javForBloomFilter);
            if (Switches.STATS) {
                buildBloomFiltersTime += (System.nanoTime() - buildBloomFiltersTimeMarker);
            }
            JoinValueContainerKey jav = extract(r2, false);
            if (!hashTableH.containsKey(jav)) {
                hashTableH.put(jav, new LinkedList<>());
            }
            hashTableH.get(jav).add(r2);
        }
        if (Switches.STATS) {
            statisticsInformation.setApproxNumberOfTuplesInBloomFilter(bloomFilter.approximateElementCount());
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("(approx) number of elements in bloom Filter: " + bloomFilter.approximateElementCount()));
        }
        if (Switches.STATS) {
            buildBloomFiltersTimeMarker = System.nanoTime();
        }
        r1Operator.passContext(new Context(r2Operator.getSchemaTableName(), factTableJoinAttributeIdx, bloomFilter));
        if (Switches.STATS) {
            buildBloomFiltersTime += (System.nanoTime() - buildBloomFiltersTimeMarker);
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("exit " + getTraceOperatorName() + ".open()"));
            decrementTraceDepth();
        }
    }

    /**
     * Note that we assume the left-deep query plan.
     */
    @Override
    public ObjectRow passContext(OperatorInformation info)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
            incrementTraceDepth();
        }
        r1Operator.passContext(info);
        return null;
    }

    public static class Context
            implements OperatorInformation
    {
        private final SchemaTableName schemaTableName;
        private BloomFilter bloomFilter;
        private final List<Integer> factTableJoinAttributeIdx;

        public Context(SchemaTableName schemaTableName, List<Integer> factTableJoinAttributeIdx, BloomFilter bloomFilter)
        {
            requireNonNull(schemaTableName, "schemaTableName is null");
            requireNonNull(factTableJoinAttributeIdx, "factTableJoinAttributeIdx is null");
            requireNonNull(bloomFilter, "bloomFilter is null");

            this.schemaTableName = schemaTableName;
            this.factTableJoinAttributeIdx = factTableJoinAttributeIdx;
            this.bloomFilter = bloomFilter;
        }

        public BloomFilter getBloomFilter()
        {
            return bloomFilter;
        }

        public List<Integer> getFactTableJoinAttributeIdx()
        {
            return factTableJoinAttributeIdx;
        }

        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }
    }

    private JoinValueContainerKey constructJavForBloomFilters(Row row)
    {
        return new JoinValueContainerKey(
                joinIdxTmp.values().stream().map(i -> row.getVals().get(i)).collect(Collectors.toList()));
    }

    private void populateFactTableJoinAttributeIdx()
    {
        checkArgument(r2Operator.getOperatorType() == OptType.table, "assume left-deep query plan");
        factTableJoinAttributeIdx = new ArrayList<>();
        joinIdxTmp = new HashMap<>();
        requireNonNull(planBuildContext, "planBuildContext is null");
        Operator factTableOperator = planBuildContext.getLeftMostPlanNodeOperator();
        List<? extends ColumnHandle> factTableHandles = factTableOperator.getColumns();
        List<String> factTableAttributes = factTableHandles.stream().map(ColumnHandle::getColumnName).collect(Collectors.toList());
        List<Type> factTableTypes = factTableHandles.stream().map(ColumnHandle::getColumnType).collect(Collectors.toList());
        List<? extends ColumnHandle> baseRelationHandles = r2Operator.getColumns();
        baseRelationAttributes = baseRelationHandles.stream().map(ColumnHandle::getColumnName).collect(Collectors.toList());
        List<Type> baseRelationTypes = baseRelationHandles.stream().map(ColumnHandle::getColumnType).collect(Collectors.toList());

        for (int i = 0; i < factTableAttributes.size(); ++i) {
            for (int j = 0; j < baseRelationAttributes.size(); ++j) {
                if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                    traceLogger.debug(String.format("i: %s, factTableAttributes[i]: %s, factTableTypes[i]: %s",
                            i, factTableAttributes.get(i), factTableTypes.get(i)));
                    traceLogger.debug(String.format("j: %s, baseRelationAttributes[i]: %s, baseRelationTypes[i]: %s",
                            j, baseRelationAttributes.get(j), baseRelationTypes.get(j)));
                }
                if (factTableAttributes.get(i).equals(baseRelationAttributes.get(j)) &&
                        factTableTypes.get(i).equals(baseRelationTypes.get(j))) {
                    factTableJoinAttributeIdx.add(i);
                    joinIdxTmp.put(i, j);
                }
            }
        }
    }

    @Override
    protected void updateStatisticsInformatAtClose()
    {
        statisticsInformation.setBuildBloomFiltersTime(buildBloomFiltersTime);
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedLIPHashJoinOperator(this, context);
    }
}
