package org.zhu45.treetracker.mapColoring;

import lombok.Data;
import org.zhu45.treetracker.common.BaseDomain;
import org.zhu45.treetracker.common.ColorValue;

@Data
public class MapColoringDomain
        extends BaseDomain<MapColoringDomain, ColorValue>
{
    public MapColoringDomain(ColorValue... values)
    {
        super(values);
    }

    public MapColoringDomain(MapColoringDomain other)
    {
        super(other);
    }
}
