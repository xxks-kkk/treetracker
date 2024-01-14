package org.zhu45.treetracker.common;

public class TreeTrackerException
        extends RuntimeException
{
    private final ErrorCode errorCode;

    public TreeTrackerException(ErrorCodeSupplier errorCode, String message)
    {
        this(errorCode, message, null);
    }

    public TreeTrackerException(ErrorCodeSupplier errorCode, Throwable throwable)
    {
        this(errorCode, null, throwable);
    }

    public TreeTrackerException(ErrorCodeSupplier errorCodeSupplier, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }
}
