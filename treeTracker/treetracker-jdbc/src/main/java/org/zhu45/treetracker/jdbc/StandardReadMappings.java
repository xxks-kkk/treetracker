package org.zhu45.treetracker.jdbc;

import com.google.common.base.CharMatcher;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.common.type.VarcharType;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Optional;

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.type.CharType.createCharType;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.createUnboundedVarcharType;
import static org.zhu45.treetracker.common.type.VarcharType.createVarcharType;
import static org.zhu45.treetracker.jdbc.ReadMapping.longReadMapping;
import static org.zhu45.treetracker.jdbc.ReadMapping.stringReadMapping;

public final class StandardReadMappings
{
    private StandardReadMappings() {}

    public static ReadMapping charReadMapping(CharType charType)
    {
        requireNonNull(charType, "charType is null");
        return stringReadMapping(charType, (resultSet, columnIndex) -> CharMatcher.is(' ').trimTrailingFrom(resultSet.getString(columnIndex)));
    }

    public static ReadMapping varcharReadMapping(VarcharType varcharType)
    {
        requireNonNull(varcharType, "varcharType is null");
        return stringReadMapping(varcharType, (resultSet, columnIndex) -> resultSet.getString(columnIndex));
    }

    public static ReadMapping integerReadMapping()
    {
        return longReadMapping(INTEGER, ResultSet::getInt);
    }

    public static Optional<ReadMapping> jdbcTypeToTreeTrackerType(JdbcTypeHandle type)
    {
        int columnSize = type.getColumnSize();
        switch (type.getJdbcType()) {
            case Types.CHAR:
            case Types.NCHAR:
                int charLength = min(columnSize, CharType.MAX_LENGTH);
                return Optional.of(charReadMapping(createCharType(charLength)));
            case Types.VARCHAR:
                if (columnSize > VarcharType.MAX_LENGTH) {
                    return Optional.of(varcharReadMapping(createUnboundedVarcharType()));
                }
                return Optional.of(varcharReadMapping(createVarcharType(columnSize)));
            case Types.INTEGER:
                return Optional.of(integerReadMapping());
        }
        return Optional.empty();
    }
}
