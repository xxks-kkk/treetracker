package org.zhu45.treetracker.relational.planner.cost;

import lombok.Getter;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.util.List;

@Getter
public class JoinTreeCostReturn
{
    // total cost
    private final double cost;
    // cost related to size of intermediate results that are part of final join result
    private final double costSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
    // cost related to the size of inner relations that are in the clean state
    private final double innerRelationCleanStateSize;
    private final List<String> sqls;
    private final SemiJoinOrdering semiJoinOrdering;

    private JoinTreeCostReturn(Builder builder)
    {
        this.cost = builder.cost;
        this.sqls = builder.sqls;
        this.costSizeOfIntermediateResultsThatArePartOfFinalJoinResult = builder.costSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
        this.innerRelationCleanStateSize = builder.innerRelationCleanStateSize;
        this.semiJoinOrdering = builder.semiJoinOrdering;
    }

    @Override
    public String toString()
    {
        return "JoinTreeCostReturn{" +
                "cost=" + cost +
                ", sqls=" + sqls +
                ", costSizeOfIntermediateResultsThatArePartOfFinalJoinResult=" + costSizeOfIntermediateResultsThatArePartOfFinalJoinResult +
                ", innerRelationCleanStateSize=" + innerRelationCleanStateSize +
                ", semijoinOrdering=" + semiJoinOrdering +
                '}';
    }

    public static Builder builder(double cost)
    {
        return new Builder(cost);
    }

    public static class Builder
    {
        private final double cost;
        private List<String> sqls;
        private double costSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
        private double innerRelationCleanStateSize;
        private SemiJoinOrdering semiJoinOrdering;

        private Builder(double cost)
        {
            this.cost = cost;
        }

        public Builder setSql(List<String> sqls)
        {
            this.sqls = sqls;
            return this;
        }

        public Builder setCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult(double costSizeOfIntermediateResultsThatArePartOfFinalJoinResult)
        {
            this.costSizeOfIntermediateResultsThatArePartOfFinalJoinResult = costSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
            return this;
        }

        public Builder innerRelationCleanStateSize(double innerRelationCleanStateSize)
        {
            this.innerRelationCleanStateSize = innerRelationCleanStateSize;
            return this;
        }

        public Builder setSemijoinOrdering(SemiJoinOrdering semiJoinOrdering)
        {
            this.semiJoinOrdering = semiJoinOrdering;
            return this;
        }

        public JoinTreeCostReturn build()
        {
            return new JoinTreeCostReturn(this);
        }
    }
}
