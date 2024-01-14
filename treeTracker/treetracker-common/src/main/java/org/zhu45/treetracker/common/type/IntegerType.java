package org.zhu45.treetracker.common.type;

import java.io.Serializable;

public class IntegerType
        extends AbstractIntType
        implements Serializable
{
    public static final IntegerType INTEGER = new IntegerType();

    private IntegerType()
    {
        super(long.class);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == INTEGER;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }

    @Override
    public int getLength()
    {
        return 4;
    }
}
