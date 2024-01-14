package org.zhu45.treetracker.common;

import java.util.Objects;
import java.util.Optional;

/**
 * Undirected edge
 */
public class Edge<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
{
    private final T node1;
    private final T node2;
    private EdgeWeight edgeWeight;

    public Edge(T node1, T node2, Optional<EdgeWeight> edgeWeight)
    {
        this.node1 = node1;
        this.node2 = node2;
        edgeWeight.ifPresent(weight -> this.edgeWeight = weight);
    }

    public Edge(T tail, T head)
    {
        this(tail, head, Optional.empty());
    }

    public T getNode1()
    {
        return node1;
    }

    public T getNode2()
    {
        return node2;
    }

    public static <T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>> Edge<T, V, D> asEdge(T tail, T head)
    {
        return new Edge<>(tail, head);
    }

    public static <T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>> Edge<T, V, D> asEdge(T tail, T head, EdgeWeight edgeWeight)
    {
        return new Edge<>(tail, head, Optional.of(edgeWeight));
    }

    public EdgeWeight getEdgeWeight()
    {
        return edgeWeight;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(node1, node2, edgeWeight);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Edge other = (Edge) obj;
        return Objects.equals(this.node1, other.node1) &&
                Objects.equals(this.node2, other.node2) &&
                Objects.equals(this.edgeWeight, other.edgeWeight);
    }
}
