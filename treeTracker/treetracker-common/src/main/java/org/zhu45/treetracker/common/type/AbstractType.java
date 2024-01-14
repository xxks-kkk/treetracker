package org.zhu45.treetracker.common.type;

public class AbstractType
        implements Type
{
    @Override
    public String getDisplayName()
    {
        throw new UnsupportedOperationException();
    }

    private final Class<?> javaType;

    protected AbstractType(Class<?> javaType)
    {
        this.javaType = javaType;
    }

    @Override
    public Class<?> getJavaType()
    {
        return this.javaType;
    }

    @Override
    public int getLength()
    {
        throw new UnsupportedOperationException();
    }
}
