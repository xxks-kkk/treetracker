package org.zhu45.treetracker.common;

import java.io.Serializable;

public class NullValue
        implements Value, Serializable
{
    public NullValue() {}

    @Override
    public String toString()
    {
        return "null";
    }
}
