package org.zhu45.treetracker.relational.planner.cost;

import lombok.Getter;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import static java.util.Objects.requireNonNull;

@Getter
public class CardEstReturn
{
    private final long size;
    private final MultiwayJoinOrderedGraph joinTree;
    private final JoinOrdering joinOrdering;
    private final JoinTreeCostReturn costReturn;

    private CardEstReturn(Builder builder)
    {
        size = builder.size;
        joinTree = builder.joinTree;
        joinOrdering = builder.joinOrdering;
        costReturn = builder.costReturn;
    }

    @Override
    public String toString()
    {
        return "CardEstReturn{" +
                "size=" + size +
                ", joinTree=" + joinTree +
                ", joinOrdering=" + joinOrdering +
                ", costReturn=" + costReturn +
                '}';
    }

    public static Builder builder(long size)
    {
        return new Builder(size);
    }

    public static class Builder
    {
        private long size;
        private MultiwayJoinOrderedGraph joinTree;
        private JoinOrdering joinOrdering;
        private JoinTreeCostReturn costReturn;

        private Builder(long size)
        {
            this.size = size;
        }

        public Builder setJoinTree(MultiwayJoinOrderedGraph joinTree)
        {
            this.joinTree = requireNonNull(joinTree);
            return this;
        }

        public Builder setJoinOrdering(JoinOrdering joinOrdering)
        {
            this.joinOrdering = joinOrdering;
            return this;
        }

        public Builder setCostReturn(JoinTreeCostReturn costReturn)
        {
            this.costReturn = costReturn;
            return this;
        }

        public CardEstReturn build()
        {
            return new CardEstReturn(this);
        }
    }
}
