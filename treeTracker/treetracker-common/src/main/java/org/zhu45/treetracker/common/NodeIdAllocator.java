package org.zhu45.treetracker.common;

public class NodeIdAllocator
{
    private static int id;

    public NodeIdAllocator() {}

    public int getNextId()
    {
        id += 1;
        return id;
    }
}
