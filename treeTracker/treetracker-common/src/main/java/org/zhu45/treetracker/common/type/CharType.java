package org.zhu45.treetracker.common.type;

import java.io.Serializable;
import java.util.Objects;

import static java.lang.String.format;

public class CharType
        extends AbstractVariableWidthType
        implements Serializable
{
    public static final int MAX_LENGTH = 65_536;
    public static final CharType CHAR = new CharType(MAX_LENGTH);

    private final int length;

    public static CharType createCharType(long length)
    {
        return new CharType(length);
    }

    private CharType(long length)
    {
        super(String.class);
        if (length < 0 || length > MAX_LENGTH) {
            throw new IllegalArgumentException(format("CHAR length scale must be in range [0, %s]", MAX_LENGTH));
        }
        this.length = (int) length;
    }

    @Override
    public int getLength()
    {
        return length;
    }

    @Override
    public String getDisplayName()
    {
        return String.format("char(%s)", length);
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

        CharType other = (CharType) o;

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
