package org.zhu45.treetracker.jdbc;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class JdbcTypeHandle
{
    private final int jdbcType;
    private final int columnSize;
    private final int decimalDigits;

    public JdbcTypeHandle(
            int jdbcType,
            int columnSize,
            int decimalDigits)
    {
        this.jdbcType = jdbcType;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
    }

    public int getJdbcType()
    {
        return jdbcType;
    }

    public int getColumnSize()
    {
        return columnSize;
    }

    public int getDecimalDigits()
    {
        return decimalDigits;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(jdbcType, columnSize, decimalDigits);
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
        JdbcTypeHandle that = (JdbcTypeHandle) o;
        return jdbcType == that.jdbcType &&
                columnSize == that.columnSize &&
                decimalDigits == that.decimalDigits;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("jdbcType", jdbcType)
                .add("columnSize", columnSize)
                .add("decimalDigits", decimalDigits)
                .toString();
    }
}
