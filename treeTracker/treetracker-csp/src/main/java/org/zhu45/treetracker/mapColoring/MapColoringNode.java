package org.zhu45.treetracker.mapColoring;

import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treetracker.common.ColorValue;

public class MapColoringNode
        extends BaseNode<MapColoringNode, ColorValue, MapColoringDomain>
{
    public MapColoringNode(String nodeName, MapColoringDomain domain)
    {
        super(nodeName, domain);
    }

    @Override
    public String toString()
    {
        return super.getNodeName();
    }
}
