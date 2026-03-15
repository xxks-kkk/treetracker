package org.zhu45.treetracker.relational;

import org.zhu45.treetracker.common.Value;

import java.io.Serializable;
import java.util.Arrays;

public class JoinValueContainerIntKey
        implements Value, Serializable
{
    private int key;
    private int[] vals;

    public JoinValueContainerIntKey(int[] vals)
    {
        this.vals = vals;
        key = Arrays.hashCode(vals);
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
        JoinValueContainerIntKey o = (JoinValueContainerIntKey) obj;
        if (o.key != this.key) {
            return false;
        }
        return Arrays.equals(o.vals, this.vals);
    }

    public void setVals(int[] vals)
    {
        this.vals = vals;
        this.key = Arrays.hashCode(this.vals);
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
