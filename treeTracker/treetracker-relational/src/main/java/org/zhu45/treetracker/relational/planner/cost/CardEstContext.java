package org.zhu45.treetracker.relational.planner.cost;

import lombok.Getter;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;

@Getter
public class CardEstContext
{
    private final PlanBuildContext planBuildContext;
    private MultiwayJoinOrderedGraph existingJoinTree;
    private Integer traceDepth;

    private CardEstContext(Builder builder)
    {
        planBuildContext = builder.planBuildContext;
    }

    public void setExistingJoinTree(MultiwayJoinOrderedGraph joinTree)
    {
        this.existingJoinTree = joinTree;
    }

    public void setTraceDepth(Integer traceDepth)
    {
        this.traceDepth = traceDepth;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private PlanBuildContext planBuildContext;

        private Builder()
        {
        }

        public Builder setPlanBuildContext(PlanBuildContext planBuildContext)
        {
            this.planBuildContext = planBuildContext;
            return this;
        }

        public CardEstContext build()
        {
            return new CardEstContext(this);
        }
    }
}
