package org.zhu45.treetracker.relational.planner.cost;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;

@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JoinTreeCostStatistics
{
    private final String providerName;
    private final int numCacheHit;
    private final int numSQLsGen;
    private final int numJoinTreeCosted;

    private JoinTreeCostStatistics(Builder builder)
    {
        this.providerName = builder.providerName;
        this.numCacheHit = builder.numCacheHit;
        this.numSQLsGen = builder.numSQLsGen;
        this.numJoinTreeCosted = builder.numJoinTreeCosted;
    }

    public static Builder builder(Class<? extends JoinTreeCostProvider> joinTreeCostProviderClazz)
    {
        return new Builder(joinTreeCostProviderClazz);
    }

    public static class Builder
    {
        private final String providerName;
        private int numCacheHit;
        private int numSQLsGen;
        private int numJoinTreeCosted;

        public Builder(Class<? extends JoinTreeCostProvider> joinTreeCostProviderClazz)
        {
            this.providerName = joinTreeCostProviderClazz.getCanonicalName();
        }

        public Builder cacheHit(int numCacheHit)
        {
            this.numCacheHit = numCacheHit;
            return this;
        }

        public Builder numSQLsGen(int numSQLsGen)
        {
            this.numSQLsGen = numSQLsGen;
            return this;
        }

        public Builder numJoinTreeCosted(int numJoinTreeCosted)
        {
            this.numJoinTreeCosted = numJoinTreeCosted;
            return this;
        }

        public JoinTreeCostStatistics build()
        {
            return new JoinTreeCostStatistics(this);
        }
    }
}
