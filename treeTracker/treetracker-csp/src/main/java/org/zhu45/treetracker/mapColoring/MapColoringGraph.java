package org.zhu45.treetracker.mapColoring;

import lombok.Getter;
import org.zhu45.treetracker.common.ColorValue;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.Graph;

import java.util.List;

@Getter
public class MapColoringGraph
        extends Graph<MapColoringNode, ColorValue, MapColoringDomain>
{
    public MapColoringGraph(List<Edge<MapColoringNode, ColorValue, MapColoringDomain>> edgeList)
    {
        super(edgeList);
    }
}
