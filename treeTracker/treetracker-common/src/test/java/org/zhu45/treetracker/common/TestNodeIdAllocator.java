package org.zhu45.treetracker.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestNodeIdAllocator
{
    @Test
    public void testNodeIdAllocator() {
        NodeIdAllocator allocator = new NodeIdAllocator();
        int nodeId1 = allocator.getNextId();
        int nodeId2 = allocator.getNextId();
        assertTrue(nodeId2 > nodeId1);
    }

    @Test
    public void testNodeIdSupplier() {
        NodeIdAllocator nodeIdAllocator1 = NodeIdSupplier.nodeIdSupplier.get();
        NodeIdAllocator nodeIdAllocator2 = NodeIdSupplier.nodeIdSupplier.get();
        assertSame(nodeIdAllocator2, nodeIdAllocator1);
    }
}
