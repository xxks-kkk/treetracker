package org.zhu45.treetracker.mapColoring;

import org.zhu45.treetracker.common.ColorValue;
import org.zhu45.treetracker.common.Constraints;

import static java.util.Objects.requireNonNull;

public class MapColoringConstraints
        extends Constraints<MapColoringNode, ColorValue, MapColoringDomain>
{
    public MapColoringConstraints()
    {
    }

    /**
     * We follow the instantiation definition from
     * Bayardo Jr, Roberto J., and Daniel P. Miranker.
     * "An optimal backtrack algorithm for tree-structured constraint satisfaction problems."
     * Artificial Intelligence 71.1 (1994): 159-181.
     * <p>
     * A variable is said to be instantiated if it is assigned a domain value which satisfies all constraints
     * defined over it and the variables previous to it along the ordering.
     * <p>
     * This method specific checks whether the given val can instantiate the given baseNode in CSP graph (i.e., variable in
     * CSP problem)
     *
     * @param baseNode the baseNode that value will be used to instantiate
     * @param val the value that is used to try to instantiate the baseNode
     * @return true if the given val can instantiate the baseNode; false otherwise.
     */
    public boolean isInstantiate(MapColoringNode baseNode, ColorValue val)
    {
        for (MapColoringNode neighborBaseNode : baseNode.getConnected()) {
            if (neighborBaseNode.getAssignedValue() != null && neighborBaseNode.getAssignedValue().equals(val)) {
                return false;
            }
        }
        return true;
    }

    public boolean isInstantiate(MapColoringNode child, MapColoringNode parent, ColorValue val)
    {
        requireNonNull(child, "supplied child node is null");
        requireNonNull(parent, "supplied parent node is null");
        if (parent.getAssignedValue() != null && parent.getAssignedValue().equals(val)) {
            return false;
        }
        return true;
    }
}
