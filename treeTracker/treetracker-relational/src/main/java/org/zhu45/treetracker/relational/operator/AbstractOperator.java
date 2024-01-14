package org.zhu45.treetracker.relational.operator;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StandardErrorCode;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodListMap;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNodeId;
import org.zhu45.treetracker.relational.planner.catalog.TableCatalog;

import static org.zhu45.treetracker.common.Utils.formatTraceMessageWithDepth;

public abstract class AbstractOperator
        implements Operator
{
    private PlanNodeId operatorId;
    private Integer traceDepth;
    private final String operatorName = this.getClass().getName();
    protected PlanBuildContext planBuildContext;
    protected StatisticsInformation statisticsInformation;
    // size of relation size set during the plan generation
    // for join operator, this is R_{inner} size; for table scan operator, this is associated relation size
    protected long operatorAssociatedRelationSize;

    public String getOperatorName()
    {
        return operatorName;
    }

    public void setOperatorID(PlanNodeId id)
    {
        operatorId = id;
    }

    public PlanNodeId getOperatorID()
    {
        return operatorId;
    }

    public void swap()
    {
        throw new TreeTrackerException(StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING, "swap is not implemented");
    }

    public OperatorInformation getOperatorInfo()
    {
        return null;
    }

    public void setOperatorTraceDepth(Integer traceDepth)
    {
        this.traceDepth = traceDepth;
    }

    @Override
    public Integer getOperatorTraceDepth()
    {
        return traceDepth;
    }

    public void incrementTraceDepth()
    {
        traceDepth++;
    }

    public void decrementTraceDepth()
    {
        traceDepth--;
    }

    public String formatTraceMessage(String message)
    {
        return formatTraceMessageWithDepth(message, traceDepth);
    }

    public Row passContext(OperatorInformation information)
    {
        return null;
    }

    public void setMultiwayJoinNode(MultiwayJoinNode node)
    {
    }

    public MultiwayJoinNode getMultiwayJoinNode()
    {
        return null;
    }

    public void setPlanBuildContext(PlanBuildContext planBuildContext)
    {
        this.planBuildContext = planBuildContext;
    }

    public PlanBuildContext getPlanBuildContext()
    {
        return this.planBuildContext;
    }

    public StatisticsInformation getStatisticsInformation()
    {
        return statisticsInformation;
    }

    public void setNoGoodListMap(NoGoodListMap noGoodListMap)
    {
        throw new UnsupportedOperationException();
    }

    public NoGoodListMap getNoGoodListMap()
    {
        throw new UnsupportedOperationException();
    }

    public void setUseDomainAsSource(boolean useDomainAsSource)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getUseDomainAsSource()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLeftMostOperatorInPlan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLeftMostOperatorInPlan()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOperatorAssociatedRelationSize(long operatorAssociatedRelationSize)
    {
        this.operatorAssociatedRelationSize = operatorAssociatedRelationSize;
    }

    @Override
    public long getOperatorAssociatedRelationSize()
    {
        return operatorAssociatedRelationSize;
    }

    @Override
    public void setSchemaTableName(SchemaTableName schemaTableName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTableCatalog(TableCatalog tableCatalog)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void initializeContextObject()
    {
        throw new UnsupportedOperationException();
    }
}
