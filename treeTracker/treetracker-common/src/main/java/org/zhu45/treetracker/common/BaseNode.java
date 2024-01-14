package org.zhu45.treetracker.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.zhu45.treetracker.common.NodeIdSupplier.nodeIdSupplier;

public class BaseNode<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
        implements Node<T, V, D>, Serializable
{
    @JsonIgnore
    protected Domain<D, V> domain;
    @JsonIgnore
    private V assignedValue;
    @JsonIgnore
    private List<T> connected;
    @JsonInclude
    private String nodeName;
    @JsonIgnore
    private NodeType nodeType;
    @JsonIgnore
    private final int nodeId;

    public BaseNode(String nodeName, Domain<D, V> domain)
    {
        this.nodeName = nodeName;
        this.domain = domain;
        this.connected = new ArrayList<>();
        this.nodeType = NodeType.None;
        this.nodeId = nodeIdSupplier.get().getNextId();
    }

    public BaseNode(T baseNode)
    {
        this.nodeName = baseNode.getNodeName();
        this.domain = baseNode.getDomain().deepCopy();
        // Purposefully not copied connected because it may contain references to existing nodes,
        // which may not get deep copied. Thus, the cloned baseNode may have dependency on the existing nodes.
        // This field should be fixed by the caller by maintaining connected relation using cloned nodes.
        this.connected = new ArrayList<>();
        this.assignedValue = baseNode.getAssignedValue();
        this.nodeType = baseNode.getNodeType();
        this.nodeId = nodeIdSupplier.get().getNextId();
    }

    public void addConnected(T baseNode)
    {
        this.connected.add(baseNode);
    }

    /**
     * @return A string representation of connected field
     */
    public String connectedToString()
    {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < connected.size(); ++i) {
            if (i != connected.size() - 1) {
                str.append(connected.get(i).getNodeName()).append(",");
            }
            else {
                str.append(connected.get(i).getNodeName());
            }
        }
        return str.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (!(o instanceof BaseNode)) {
            return false;
        }
        T c = (T) o;
        return c.getNodeName().equals(this.nodeName) &&
                Objects.equals(c.getDomain(), this.domain) &&
                c.getConnected().hashCode() == this.getConnected().hashCode() &&
                Objects.equals(c.getAssignedValue(), this.assignedValue);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(nodeName);
    }

    @Override
    public String toString()
    {
        return this.nodeName;
    }

    public Domain<D, V> getDomain()
    {
        return this.domain;
    }

    public V getAssignedValue()
    {
        return this.assignedValue;
    }

    public List<T> getConnected()
    {
        return this.connected;
    }

    public String getNodeName()
    {
        return this.nodeName;
    }

    public void setDomain(Domain<D, V> domain)
    {
        this.domain = domain;
    }

    public void setAssignedValue(V assignedValue)
    {
        this.assignedValue = assignedValue;
    }

    public void setConnected(List<T> connected)
    {
        this.connected = connected;
    }

    public void setNodeName(String nodeName)
    {
        this.nodeName = nodeName;
    }

    public NodeType getNodeType()
    {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType)
    {
        this.nodeType = nodeType;
    }

    public int getNodeId()
    {
        return nodeId;
    }
}
