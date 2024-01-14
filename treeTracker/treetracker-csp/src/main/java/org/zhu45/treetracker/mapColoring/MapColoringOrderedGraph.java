package org.zhu45.treetracker.mapColoring;

import org.zhu45.treektracker.multiwayJoin.BaseOrderedGraph;
import org.zhu45.treetracker.common.ColorValue;

import java.util.HashMap;
import java.util.List;

public class MapColoringOrderedGraph
        extends BaseOrderedGraph<MapColoringNode, ColorValue, MapColoringDomain>
{
    public MapColoringOrderedGraph(MapColoringNode root,
            HashMap<MapColoringNode, List<MapColoringNode>> children,
            HashMap<MapColoringNode, List<MapColoringNode>> parent,
            List<MapColoringNode> traversalList)
    {
        super(root, children, parent, traversalList);
    }

    public MapColoringOrderedGraph(MapColoringOrderedGraph og)
    {
        super(og);
    }
}
