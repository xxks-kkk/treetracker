package org.zhu45.treetracker.relational.planner.rule;

import lombok.Getter;
import lombok.Setter;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNodeIdAllocator;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;

import java.util.List;
import java.util.Map;

public class RuleContext
{
    private final Operator operator;
    private final PlanStatistics planStatistics;
    private final JdbcClient jdbcClient;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<OptType, List<Class<? extends Operator>>> operatorMap;
    private final PlanBuildContext planBuildContext;
    private final Plan plan;
    // Sometimes, we just want to reuse part of rule logic without need to actually create a new plan.
    @Getter @Setter
    private boolean notGeneratePlan;

    private RuleContext(Builder builder)
    {
        this.operator = builder.operator;
        this.planStatistics = builder.planStatistics;
        this.jdbcClient = builder.jdbcClient;
        this.idAllocator = builder.idAllocator;
        this.operatorMap = builder.operatorMap;
        this.planBuildContext = builder.planBuildContext;
        this.plan = builder.plan;
    }

    public Operator getOperator()
    {
        return operator;
    }

    public Plan getPlan()
    {
        return plan;
    }

    public PlanStatistics getPlanStatistics()
    {
        return planStatistics;
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    public PlanNodeIdAllocator getPlanNodeIdAllocator()
    {
        return idAllocator;
    }

    public Map<OptType, List<Class<? extends Operator>>> getOperatorMap()
    {
        return operatorMap;
    }

    public PlanBuildContext getPlanBuildContext()
    {
        return planBuildContext;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Operator operator;
        private PlanStatistics planStatistics;
        private JdbcClient jdbcClient;
        private PlanNodeIdAllocator idAllocator;
        private Map<OptType, List<Class<? extends Operator>>> operatorMap;
        private PlanBuildContext planBuildContext;
        private Plan plan;

        public Builder()
        {
        }

        public Builder setOperator(Operator operator)
        {
            this.operator = operator;
            return this;
        }

        public Builder setPlanStatistics(PlanStatistics planStatistics)
        {
            this.planStatistics = planStatistics;
            return this;
        }

        public Builder setJdbcClinet(JdbcClient jdbcClient)
        {
            this.jdbcClient = jdbcClient;
            return this;
        }

        public Builder setPlanNodeIdAllocator(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
            return this;
        }

        public Builder setOperatorMap(Map<OptType, List<Class<? extends Operator>>> operatorMap)
        {
            this.operatorMap = operatorMap;
            return this;
        }

        public Builder setPlanBuildContext(PlanBuildContext planBuildContext)
        {
            this.planBuildContext = planBuildContext;
            return this;
        }

        public Builder setPlan(Plan plan)
        {
            this.plan = plan;
            return this;
        }

        public RuleContext build()
        {
            return new RuleContext(this);
        }
    }
}
