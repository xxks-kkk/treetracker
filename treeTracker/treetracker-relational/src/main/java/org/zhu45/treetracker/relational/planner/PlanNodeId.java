package org.zhu45.treetracker.relational.planner;

import static java.util.Objects.requireNonNull;

public class PlanNodeId
{
    private final String id;

    public PlanNodeId(String id)
    {
        requireNonNull(id, "id is null");
        this.id = id;
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PlanNodeId that = (PlanNodeId) o;

        return id.equals(that.id);
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }
}
