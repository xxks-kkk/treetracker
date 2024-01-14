package org.zhu45.treetracker.common;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

public class NodeIdSupplier
{
    private NodeIdSupplier() {}

    public static Supplier<NodeIdAllocator> nodeIdSupplier = Suppliers.memoize(NodeIdAllocator::new);
}
