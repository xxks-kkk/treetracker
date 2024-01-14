package org.zhu45.treetracker.common;

import java.util.List;

public interface Node<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
{
    void addConnected(T node);

    String connectedToString();

    Domain<D, V> getDomain();

    default V getAssignedValue()
    {
        throw new UnsupportedOperationException();
    }

    List<T> getConnected();

    String getNodeName();

    void setDomain(Domain<D, V> domain);

    default void setAssignedValue(V assignedValue)
    {
        throw new UnsupportedOperationException();
    }

    void setConnected(List<T> connected);

    void setNodeName(String nodeName);

    NodeType getNodeType();

    void setNodeType(NodeType nodeType);

    int getNodeId();
}
