package org.zhu45.treetracker.common.type;

import java.io.Serializable;
import java.util.Objects;

public class VarcharType
        extends AbstractVariableWidthType
        implements Serializable
{
    public static final int UNBOUNDED_LENGTH = Integer.MAX_VALUE;
    public static final int MAX_LENGTH = Integer.MAX_VALUE - 1;
    public static final VarcharType VARCHAR = new VarcharType(UNBOUNDED_LENGTH);

    public static VarcharType createUnboundedVarcharType()
    {
        return VARCHAR;
    }

    public static VarcharType createVarcharType(int length)
    {
        if (length > MAX_LENGTH || length < 0) {
            // Use createUnboundedVarcharType for unbounded VARCHAR.
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        return new VarcharType(length);
    }

    private final int length;

    private VarcharType(int length)
    {
        super(String.class);
        if (length < 0) {
            throw new IllegalArgumentException("Invalid VARCHAR length " + length);
        }
        this.length = length;
    }

    public int getLength()
    {
        if (isUnbounded()) {
            return UNBOUNDED_LENGTH;
        }
        return length;
    }

    public boolean isUnbounded()
    {
        return length == UNBOUNDED_LENGTH;
    }

    @Override
    public String getDisplayName()
    {
        if (isUnbounded()) {
            return "varchar";
        }
        return String.format("varchar(%s)", length);
    }

    public static boolean isVarcharType(Type type)
    {
        return type instanceof VarcharType;
    }

    public int getLengthSafe()
    {
        if (isUnbounded()) {
            throw new IllegalStateException("Cannot get size of unbounded VARCHAR.");
        }
        return length;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VarcharType other = (VarcharType) o;

        return Objects.equals(this.length, other.length);
    }

    @Override
    public String toString()
    {
        return getDisplayName();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(length);
    }
}
