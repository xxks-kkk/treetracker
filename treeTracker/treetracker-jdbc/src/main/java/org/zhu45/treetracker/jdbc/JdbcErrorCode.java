package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.ErrorCode;
import org.zhu45.treetracker.common.ErrorCodeSupplier;
import org.zhu45.treetracker.common.ErrorType;

import static org.zhu45.treetracker.common.ErrorType.EXTERNAL;

public enum JdbcErrorCode
        implements ErrorCodeSupplier
{
    JDBC_ERROR(0, EXTERNAL),
    JDBC_NON_TRANSIENT_ERROR(1, EXTERNAL);

    private final ErrorCode errorCode;

    JdbcErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0400_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
