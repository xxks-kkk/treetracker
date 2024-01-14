package org.zhu45.treetracker.relational.operator;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.RecordTupleSourceProvider;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodListMap;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNodeId;
import org.zhu45.treetracker.relational.planner.catalog.TableCatalog;

import java.util.List;

public interface Operator
        extends AutoCloseable
{
    <C> void accept(OperatorVisitor<C> visitor, C context);

    String getOperatorName();

    void open();

    Row getNext();

    void close();

    void setChildren(List<Operator> children);

    void reset();

    void setTableCatalog(TableCatalog tableCatalog);

    List<? extends ColumnHandle> getColumns();

    OptType getOperatorType();

    void setOperatorID(PlanNodeId id);

    PlanNodeId getOperatorID();

    // Swap r1 and r2 binding. By default, r1 binds to left and r2 binds to right.
    // In some situation, we may need to swap: r1 binds to right and r2 binds to left.
    // For example, in the right-deep join tree, r1 binds to right leads to most efficient execution.
    void swap();

    OperatorInformation getOperatorInfo();

    String getTraceOperatorName();

    void setOperatorTraceDepth(Integer traceDepth);

    Integer getOperatorTraceDepth();

    Row passContext(OperatorInformation info);

    void setMultiwayJoinNode(MultiwayJoinNode node);

    void setSchemaTableName(SchemaTableName schemaTableName);

    SchemaTableName getSchemaTableName();

    MultiwayJoinNode getMultiwayJoinNode();

    void setPlanBuildContext(PlanBuildContext context);

    PlanBuildContext getPlanBuildContext();

    void setNoGoodListMap(NoGoodListMap noGoodListMap);

    NoGoodListMap getNoGoodListMap();

    void setUseDomainAsSource(boolean useDomainAsSource);

    boolean getUseDomainAsSource();

    StatisticsInformation getStatisticsInformation();

    void setLeftMostOperatorInPlan();

    boolean isLeftMostOperatorInPlan();

    void setOperatorAssociatedRelationSize(long operatorAssociatedRelationSize);

    long getOperatorAssociatedRelationSize();

    default void setRecordTupleSourceProvider(RecordTupleSourceProvider recordTupleSourceProvider)
    {
        throw new UnsupportedOperationException();
    }

    default Class<? extends RecordTupleSourceProvider> getRecordTupleSourceProviderClazz()
    {
        throw new UnsupportedOperationException();
    }

    void initializeContextObject();
}
