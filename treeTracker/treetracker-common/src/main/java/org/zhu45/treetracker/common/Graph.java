package org.zhu45.treetracker.common;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.inject.internal.util.Preconditions.checkArgument;

public class Graph<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
{
    private Set<T> nodes;
    private List<? extends Edge<T, V, D>> edgeList;

    public Graph(List<? extends Edge<T, V, D>> edgeList)
    {
        this.edgeList = edgeList;
        this.nodes = new HashSet<>();
        edgeList.forEach(edge -> {
            T node1 = edge.getNode1();
            T node2 = edge.getNode2();
            node1.addConnected(node2);
            node2.addConnected(node1);
            checkArgument(!node1.equals(node2), String.format("%s and %s are equal", node1, node2));
            nodes.add(node1);
            nodes.add(node2);
        });
    }

    public Graph(T node)
    {
        this.nodes = new HashSet<>();
        nodes.add(node);
    }

    public Set<T> getNodes()
    {
        return this.nodes;
    }

    public List<? extends Edge<T, V, D>> getEdgeList()
    {
        return edgeList;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        for (T node : nodes) {
            builder.append("BaseNode: ").append(node.toString()).append("\n");
            builder.append("connected with: ").append(node.connectedToString()).append("\n");
        }
        return builder.toString();
    }
}
