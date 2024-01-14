package org.zhu45.treetracker.relational.operator;

import lombok.Getter;
import org.zhu45.treetracker.relational.statistics.CostModel;

public class AggregateStatisticsInformationContext
{
    @Getter
    Operator rootOperator;
    @Getter
    long resultSetSize;
    @Getter
    JoinOperator algorithm;
    @Getter
    String queryName;
    @Getter
    int numRelations;
    @Getter
    long runtime;
    @Getter
    CostModel costModel;
    @Getter
    long evaluationMemoryCostInBytes;

    private AggregateStatisticsInformationContext(Builder builder)
    {
        rootOperator = builder.rootOperator;
        resultSetSize = builder.resultSetSize;
        algorithm = builder.algorithm;
        queryName = builder.queryName;
        numRelations = builder.numRelations;
        runtime = builder.runtime;
        costModel = builder.costModel;
        evaluationMemoryCostInBytes = builder.evaluationMemoryCostInBytes;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        Operator rootOperator;
        long resultSetSize;
        JoinOperator algorithm;
        String queryName;
        int numRelations;
        long runtime;
        CostModel costModel;
        long evaluationMemoryCostInBytes;

        public Builder()
        {
        }

        public Builder setRootOperator(Operator rootOperator)
        {
            this.rootOperator = rootOperator;
            return this;
        }

        public Builder setResultSetSize(long resultSetSize)
        {
            this.resultSetSize = resultSetSize;
            return this;
        }

        public Builder setJoinOperator(JoinOperator algorithm)
        {
            this.algorithm = algorithm;
            return this;
        }

        public Builder setQueryName(String queryName)
        {
            this.queryName = queryName;
            return this;
        }

        public Builder setNumRelations(int numRelations)
        {
            this.numRelations = numRelations;
            return this;
        }

        public Builder setRuntime(long runtime)
        {
            this.runtime = runtime;
            return this;
        }

        public Builder setCostModel(CostModel costModel)
        {
            this.costModel = costModel;
            return this;
        }

        public Builder setEvaluationMemoryCostInBytes(long evaluationMemoryCostInBytes)
        {
            this.evaluationMemoryCostInBytes = evaluationMemoryCostInBytes;
            return this;
        }

        public AggregateStatisticsInformationContext build()
        {
            return new AggregateStatisticsInformationContext(this);
        }
    }
}
