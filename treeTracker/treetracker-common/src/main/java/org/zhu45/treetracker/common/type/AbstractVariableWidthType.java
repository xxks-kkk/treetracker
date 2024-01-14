package org.zhu45.treetracker.common.type;

public class AbstractVariableWidthType
        extends AbstractType
        implements VariableWidthType
{
    protected AbstractVariableWidthType(Class<?> javaType)
    {
        super(javaType);
    }

    @Override
    public int getLength()
    {
        return super.getLength();
    }
}
