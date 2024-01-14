package org.zhu45.treetracker.common;

import java.util.List;

public abstract class Constraints<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
{
    public abstract boolean isInstantiate(T baseNode, V val);

    /**
     * Check if child can be assigned to a value given its parent node. It can be used as a generic method as well.
     *
     * @param child node to be assigned to val
     * @param parent node which child node check against.
     * @param val the value that is about to assign to child node
     * @return true if child node can be assigned to the given val; false otherwise
     */
    public abstract boolean isInstantiate(T child, T parent, V val);

    /**
     * Instantiate baseNode with a val from vals such that the val assigned to baseNode
     * satisfy all the (binary) constraints involving the assigned baseNode (i.e., nodes connected via edges with baseNode).
     *
     * @param baseNode the baseNode that will be assigned a val
     * @param vals a list of candidate vals and from which, a val will be picked to instantiate the baseNode
     * @return true if instantiation is successful; false otherwise (i.e., no val from vals can instantiate baseNode)
     */
    public boolean instantiate(T baseNode, List<V> vals)
    {
        for (V val : vals) {
            if (isInstantiate(baseNode, val)) {
                baseNode.setAssignedValue(val);
                return true;
            }
        }
        return false;
    }

    /**
     * Instantiate baseNode with the given val (the val may not instantiate). If the val can instantiate the baseNode,
     * baseNode will be assigned with the given val.
     *
     * @param baseNode the baseNode that to be instantiated
     * @param val the val tried to instantiate the baseNode
     * @return true if the val instantiated the baseNode; false otherwise
     */
    public boolean instantiate(T baseNode, V val)
    {
        if (isInstantiate(baseNode, val)) {
            baseNode.setAssignedValue(val);
            return true;
        }
        return false;
    }

    /**
     * Instantiate child node with val. A constraint between child and parent is checked to determine
     * if child node can be instantiated.
     *
     * @param child node that is about to be instantiated
     * @param parent node that has a constraint with child and such constraint will be checked
     * @param val value that is used to instantiate child
     * @return true if child is successfully instantiated; false otherwise
     */
    public boolean instantiate(T child, T parent, V val)
    {
        if (isInstantiate(child, parent, val)) {
            child.setAssignedValue(val);
            return true;
        }
        return false;
    }
}
