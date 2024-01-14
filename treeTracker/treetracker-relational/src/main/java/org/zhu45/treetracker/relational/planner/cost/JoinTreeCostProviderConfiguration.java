package org.zhu45.treetracker.relational.planner.cost;

import lombok.Getter;

@Getter
public class JoinTreeCostProviderConfiguration
{
    public static JoinTreeCostProviderConfiguration defaultConfiguration = JoinTreeCostProviderConfiguration.builder(EstimationMethod.SQL)
            .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(true)
            .includeInnerRelationSize(true)
            .build();

    public enum EstimationMethod
    {
        SQL
    }

    private final EstimationMethod estimationMethod;
    private final boolean useTrueCard;
    private final boolean enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
    private final boolean includeInnerRelationCleanStateSize;
    private final boolean enableGreedySemijoinOrdering;

    private JoinTreeCostProviderConfiguration(Builder builder)
    {
        this.estimationMethod = builder.estimationMethod;
        this.useTrueCard = builder.useTrueCard;
        this.enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult = builder.enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
        this.includeInnerRelationCleanStateSize = builder.includeInnerRelationCleanStateSize;
        this.enableGreedySemijoinOrdering = builder.enableGreedySemijoinOrdering;
    }

    @Override
    public String toString()
    {
        return "JoinTreeCostProviderConfiguration{" +
                "estimationMethod=" + estimationMethod +
                ", useTrueCard=" + useTrueCard +
                ", enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult=" + enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult +
                ", includeInnerRelationCleanStateSize=" + includeInnerRelationCleanStateSize +
                ", enableGreedySemijoinOrdering=" + enableGreedySemijoinOrdering +
                '}';
    }

    public static Builder builder(EstimationMethod estimationMethod)
    {
        return new Builder(estimationMethod);
    }

    public static class Builder
    {
        private EstimationMethod estimationMethod;
        private boolean useTrueCard;
        private boolean enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
        private boolean includeInnerRelationCleanStateSize;
        private boolean enableGreedySemijoinOrdering;

        private Builder(EstimationMethod estimationMethod)
        {
            this.estimationMethod = estimationMethod;
        }

        public Builder useTrueCard(boolean useTrueCard)
        {
            this.useTrueCard = useTrueCard;
            return this;
        }

        public Builder enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(boolean enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult)
        {
            this.enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult = enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
            return this;
        }

        public Builder includeInnerRelationSize(boolean includeInnerRelationCleanStateSize)
        {
            this.includeInnerRelationCleanStateSize = includeInnerRelationCleanStateSize;
            return this;
        }

        public Builder enableGreedySemijoinOrdering(boolean enableGreedySemijoinOrdering)
        {
            this.enableGreedySemijoinOrdering = enableGreedySemijoinOrdering;
            return this;
        }

        public JoinTreeCostProviderConfiguration build()
        {
            return new JoinTreeCostProviderConfiguration(this);
        }
    }
}
