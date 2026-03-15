package org.zhu45.treetracker.relational.operator;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import de.renebergelt.test.Switches;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.Utils;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.relational.JoinResultColumnHandle;
import org.zhu45.treetracker.relational.JoinValueContainerIntKey;
import org.zhu45.treetracker.relational.JoinValueContainerKey;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.plan.Side;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class TupleBasedJoinOperator
        extends AbstractOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedJoinOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private Operator left;
    private Operator right;
    protected Operator r1Operator;
    protected Operator r2Operator;

    private static final OptType operatorType = OptType.join;

    protected Row r1;
    protected Row r2;
    // record which columns (attributes) of R2 we know that are join attributes
    protected Set<Integer> processedRelationR2Column;
    // record which columns (attributes) of R2 we know that are non-join attributes
    protected int[] uniqueRelationR2Column;
    // <index of column in R1, index of column in R2> are potentially joinable
    protected ImmutableMap<Integer, Integer> joinIdx;
    // join result relation column handle containing name, type information
    protected List<JoinResultColumnHandle> resultColumnHandles;
    protected List<String> attributesList;
    protected JoinType joinType;
    protected int[] joinIdxForR1Operator;
    protected int[] joinIdxForR2Operator;

    protected StopWatch stopWatch;

    protected IntRow joinResult;
    protected JoinValueContainerIntKey javR1 = new JoinValueContainerIntKey(new int[0]);

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
        r1 = r1Operator.getNext();
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
            decrementTraceDepth();
        }
    }

    @Override
    public void close()
    {
        r1Operator.close();
        r2Operator.close();
    }

    @Override
    public void setChildren(List<Operator> children)
    {
        checkArgument(children.size() == 2, "expected newChildren to contain 2 nodes");
        checkArgument(children.get(0) != null && children.get(1) != null, "children should not contain null");
        checkArgument((children.get(0).getSide() == Side.OUTER && children.get(1).getSide() == Side.INNER) ||
                        (children.get(0).getSide() == Side.INNER && children.get(1).getSide() == Side.OUTER),
                "One operator has to be Side.INNER and the other operator has to be Side.OUTER");
        // We always want to make sure the left is the probe side.
        if (children.get(0).getSide() == Side.OUTER) {
            left = children.get(0);
            right = children.get(1);
        }
        else {
            left = children.get(1);
            right = children.get(0);
        }
        r1Operator = left;
        r2Operator = right;
    }

    @Override
    public void reset()
    {
        r1Operator.reset();
        r2Operator.reset();
    }

    @Override
    public List<? extends ColumnHandle> getColumns()
    {
        return resultColumnHandles;
    }

    @Override
    public OptType getOperatorType()
    {
        return operatorType;
    }

    @Override
    public void swap()
    {
        r1Operator = right;
        r2Operator = left;
        operatorAssociatedRelationSize = r1Operator.getOperatorAssociatedRelationSize();
    }

    @Override
    public String getTraceOperatorName()
    {
        return "join" + getOperatorID();
    }

    /**
     * Perform natural join on the given two tuples r and s.
     * The operation assumes SELECT *, which returns all attributes.
     *
     * @return A Row that represents the join tuple; null if two tuples cannot be joined
     */
    protected Row join(Row r1, Row r2)
    {
        requireNonNull(joinType, "joinType is not set; need to call construct(joinType) first");
        checkArgument(r1 != null && r2 != null, "r1 or r2 is null");

        List<RelationalValue> r1Vals = r1.getVals();
        List<RelationalValue> r2Vals = r2.getVals();
        for (int idx : joinIdx.keySet()) {
            // We don't need to call entryEqual because when building joinIdx,
            // attributes and types are already checked.
            if (!r1Vals.get(idx).equals(r2Vals.get(joinIdx.get(idx)))) {
                return null;
            }
        }
        List<RelationalValue> res = FastList.newList(r1.getVals());
        if (joinType == JoinType.NaturalJoin) {
            for (int i = 0; i < r2.size(); ++i) {
                if (!processedRelationR2Column.contains(i)) {
                    res.add(r2Vals.get(i));
                }
            }
        }
        int numResultColumns = resultColumnHandles.size();
        List<String> attributesList = FastList.newList(numResultColumns);
        List<Type> types = FastList.newList(numResultColumns);
        for (JoinResultColumnHandle columnHandle : resultColumnHandles) {
            attributesList.add(columnHandle.getColumnName());
            types.add(columnHandle.getColumnType());
        }
        return new ObjectRow(attributesList, types, res);
    }

    protected void join(IntRow r1, IntRow r2)
    {
        if (Switches.DEBUG) {
            requireNonNull(joinType, "joinType is not set; need to call construct(joinType) first");
            checkArgument(r1 != null && r2 != null, "r1 or r2 is null");
            checkArgument(joinType == JoinType.NaturalJoin, "joinType is not JoinType.NaturalJoin");
        }
        int[] r1Vals = r1.getIntVals();
        int[] r2Vals = r2.getIntVals();
        int[] res = Arrays.copyOf(r1Vals, resultColumnHandles.size());
        int counter = r1Vals.length;
        for (int j : uniqueRelationR2Column) {
            res[counter] = r2Vals[j];
            counter++;
        }
        joinResult.vals = res;
    }

    public void construct(JoinType joinType)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".open()"));
            incrementTraceDepth();
        }
        this.joinType = joinType;
        processedRelationR2Column = new HashSet<>();
        resultColumnHandles = FastList.newList();
        HashMap<Integer, Integer> joinIdxTmp = new HashMap<>();
        List<? extends ColumnHandle> r1Handles = r1Operator.getColumns();
        List<? extends ColumnHandle> r2Handles = r2Operator.getColumns();
        List<String> r1Attributes = r1Handles.stream().map(ColumnHandle::getColumnName).collect(Collectors.toList());
        List<Type> r1Types = r1Handles.stream().map(ColumnHandle::getColumnType).collect(Collectors.toList());
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("r1Types: \n" + Utils.properPrintList(r1Types)));
        }
        List<String> r2Attributes = r2Handles.stream().map(ColumnHandle::getColumnName).collect(Collectors.toList());
        List<Type> r2Types = r2Handles.stream().map(ColumnHandle::getColumnType).collect(Collectors.toList());
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("r2Types: \n" + Utils.properPrintList(r2Types)));
        }
        for (int i = 0; i < r1Handles.size(); ++i) {
            boolean iJoinWithSomeJ = false;
            for (int j = 0; j < r2Handles.size(); ++j) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("i: %s, r1Attributes[i]: %s, r1Types[i]: %s", i, r1Attributes.get(i), r1Types.get(i))));
                    traceLogger.trace(formatTraceMessage(String.format("j: %s, r2Attributes[i]: %s, r2Types[i]: %s", j, r2Attributes.get(j), r2Types.get(j))));
                }
                if (r1Attributes.get(i).equals(r2Attributes.get(j)) && r1Types.get(i).equals(r2Types.get(j))) {
                    joinIdxTmp.put(i, j);
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage("joinIdxTmp: " + joinIdxTmp));
                    }
                    processedRelationR2Column.add(j);
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage(
                                "processedRelationR2Column:\n" + Utils.properPrintList(processedRelationR2Column)));
                    }
                    resultColumnHandles.add(new JoinResultColumnHandle(r1Handles.get(i).getColumnName(),
                            r1Handles.get(i).getColumnType()));
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage(
                                "resultColumnHandles:\n" + Utils.properPrintList(resultColumnHandles)));
                    }
                    iJoinWithSomeJ = true;
                    break;
                }
            }
            if (!iJoinWithSomeJ) {
                resultColumnHandles.add(new JoinResultColumnHandle(r1Handles.get(i).getColumnName(), r1Handles.get(i).getColumnType()));
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("resultColumnHandles:\n" + Utils.properPrintList(resultColumnHandles)));
                }
            }
        }
        List<Integer> temp = new ArrayList<>();
        if (joinType == JoinType.NaturalJoin) {
            for (int i = 0; i < r2Handles.size(); ++i) {
                if (!processedRelationR2Column.contains(i)) {
                    resultColumnHandles.add(new JoinResultColumnHandle(r2Handles.get(i).getColumnName(), r2Handles.get(i).getColumnType()));
                    temp.add(i);
                }
            }
        }
        uniqueRelationR2Column = temp.stream().mapToInt(Integer::intValue).toArray();
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("resultColumnHandles: " + Utils.properPrintList(resultColumnHandles)));
        }
        joinIdx = ImmutableMap.<Integer, Integer>builder().putAll(joinIdxTmp).build();
        joinIdxForR1Operator = Ints.toArray(joinIdx.keySet());
        joinIdxForR2Operator = Ints.toArray(joinIdx.values());
        attributesList = FastList.newList(resultColumnHandles.size());
        for (JoinResultColumnHandle columnHandle : resultColumnHandles) {
            attributesList.add(columnHandle.getColumnName());
        }
        joinResult = new IntRow(attributesList, null);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            decrementTraceDepth();
        }
    }

    protected JoinValueContainerKey extract(Row row, boolean isRowFromR1Operator)
    {
        if (Switches.DEBUG) {
            requireNonNull(row, "input row is null");
        }
        int[] joinAttributeIndex = isRowFromR1Operator ? joinIdxForR1Operator : joinIdxForR2Operator;
        List<RelationalValue> vals = FastList.newList(joinAttributeIndex.length);
        List<RelationalValue> rowVals = row.getVals();
        for (int idx : joinAttributeIndex) {
            vals.add(rowVals.get(idx));
        }
        return new JoinValueContainerKey(vals);
    }

    protected void extractR1(IntRow row)
    {
        if (Switches.DEBUG) {
            requireNonNull(row, "input row is null");
        }
        int[] vals = new int[joinIdxForR1Operator.length];
        int[] rowVals = row.getIntVals();
        int counter = 0;
        for (int idx : joinIdxForR1Operator) {
            vals[counter] = rowVals[idx];
            counter++;
        }
        javR1.setVals(vals);
    }

    protected JoinValueContainerIntKey extractR2(IntRow row)
    {
        if (Switches.DEBUG) {
            requireNonNull(row, "input row is null");
        }
        int[] vals = new int[joinIdxForR2Operator.length];
        int[] rowVals = row.getIntVals();
        int counter = 0;
        for (int idx : joinIdxForR2Operator) {
            vals[counter] = rowVals[idx];
            counter++;
        }
        // Because R2 jav is used as hash keys to create a hash table, we need to create
        // a new instance; otherwise, all the hash keys will point to the same object.
        return new JoinValueContainerIntKey(vals);
    }

    @Override
    public List<JoinResultColumnHandle> getResultColumnHandles()
    {
        return resultColumnHandles;
    }
}
