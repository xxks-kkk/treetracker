package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.type.Type;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ReadMapping
{
    public static ReadMapping stringReadMapping(Type prestoType, StringReadFunction readFunction)
    {
        return new ReadMapping(prestoType, readFunction);
    }

    public static ReadMapping longReadMapping(Type prestoType, LongReadFunction readFunction)
    {
        return new ReadMapping(prestoType, readFunction);
    }

    private final Type type;
    private final ReadFunction readFunction;

    ReadMapping(Type type, ReadFunction readFunction)
    {
        this.type = requireNonNull(type, "type is null");
        this.readFunction = requireNonNull(readFunction, "readFunction is null");
        checkArgument(
                type.getJavaType() == readFunction.getJavaType(),
                "Presto type %s is not compatible with read function %s returning %s",
                type,
                readFunction,
                readFunction.getJavaType());
    }

    public Type getType()
    {
        return type;
    }

    public ReadFunction getReadFunction()
    {
        return readFunction;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .toString();
    }
}
