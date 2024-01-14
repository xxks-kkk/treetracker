package org.zhu45.treetracker.common;

import java.util.Objects;

public class DirectedEdge<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
{
    private final T head;
    private final T tail;

    public DirectedEdge(T head, T tail)
    {
        this.head = head;
        this.tail = tail;
    }

    public T getHead()
    {
        return head;
    }

    public T getTail()
    {
        return tail;
    }

    public static <T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>> DirectedEdge<T, V, D> asDirectedEdge(T head, T tail)
    {
        return new DirectedEdge<>(head, tail);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(head, tail);
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
        DirectedEdge<T, V, D> other = (DirectedEdge<T, V, D>) obj;
        return Objects.equals(this.head, other.head) &&
                Objects.equals(this.tail, other.tail);
    }
}
