package org.zhu45.treetracker.benchmark;

import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.planner.rule.Rule;

import java.util.List;

public class QueryProviderContext
{
    JoinOperator joinOperator;
    Class<? extends Query> queryClazz;
    long backJumpedRelationSize;
    int numberOfBackjumpedRelations;
    String queryName;
    boolean stopAfterFullReducer;
    List<Rule> rules;
    JdbcClient jdbcClient;

    private QueryProviderContext(Builder builder)
    {
        joinOperator = builder.joinOperator;
        queryClazz = builder.queryClazz;
        backJumpedRelationSize = builder.numberOfBackjumpedRelations;
        queryName = builder.queryName;
        numberOfBackjumpedRelations = builder.numberOfBackjumpedRelations;
        stopAfterFullReducer = builder.stopAfterFullReducer;
        rules = builder.rules;
        jdbcClient = builder.jdbcClient;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        JoinOperator joinOperator;
        Class<? extends Query> queryClazz;
        long backJumpedRelationSize;
        int numberOfBackjumpedRelations;
        String queryName;
        boolean stopAfterFullReducer;
        List<Rule> rules;
        JdbcClient jdbcClient;

        public Builder()
        {
        }

        public Builder setJdbcClient(JdbcClient jdbcClient)
        {
            this.jdbcClient = jdbcClient;
            return this;
        }

        public Builder setJoinOperator(JoinOperator algorithm)
        {
            this.joinOperator = algorithm;
            return this;
        }

        public Builder setQueryClazz(Class<? extends Query> queryClazz)
        {
            this.queryClazz = queryClazz;
            return this;
        }

        public Builder setBackJumpedRelationSize(long backJumpedRelationSize)
        {
            this.backJumpedRelationSize = backJumpedRelationSize;
            return this;
        }

        public Builder setNumberOfBackjumpedRelations(int numberOfBackjumpedRelations)
        {
            this.numberOfBackjumpedRelations = numberOfBackjumpedRelations;
            return this;
        }

        public Builder setQueryName(String queryName)
        {
            this.queryName = queryName;
            return this;
        }

        public Builder setStopAfterFullReducer(boolean stopAfterFullReducer)
        {
            this.stopAfterFullReducer = stopAfterFullReducer;
            return this;
        }

        public Builder setRules(List<Rule> rules)
        {
            this.rules = rules;
            return this;
        }

        public QueryProviderContext build()
        {
            return new QueryProviderContext(this);
        }
    }
}
