package org.zhu45.treetracker.relational;

import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.Value;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class JoinValueContainerKey
        implements Value, Serializable
{
    private final int key;
    private final List<RelationalValue> vals;

    public JoinValueContainerKey(List<RelationalValue> vals)
    {
        this.vals = vals;
        key = Objects.hash(vals);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JoinValueContainerKey o = (JoinValueContainerKey) obj;
        if (o.key != this.key) {
            return false;
        }
        return o.vals.equals(this.vals);
    }

    @Override
    public int hashCode()
    {
        return key;
    }

    @Override
    public String toString()
    {
        return vals.toString();
    }
}
