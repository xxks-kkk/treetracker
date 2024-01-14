package org.zhu45.treetracker.relational.planner;

public class PlanNodeIdAllocator
{
    private int nextId;

    public PlanNodeId getNextId()
    {
        return new PlanNodeId(Integer.toString(nextId++));
    }
}
