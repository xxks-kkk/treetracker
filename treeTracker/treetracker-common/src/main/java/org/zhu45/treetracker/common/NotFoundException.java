package org.zhu45.treetracker.common;

import static org.zhu45.treetracker.common.StandardErrorCode.NOT_FOUND;

public abstract class NotFoundException
        extends TreeTrackerException
{
    protected NotFoundException()
    {
        this(null, null);
    }

    protected NotFoundException(String message)
    {
        this(message, null);
    }

    protected NotFoundException(Throwable cause)
    {
        this(null, cause);
    }

    protected NotFoundException(String message, Throwable cause)
    {
        super(NOT_FOUND, message, cause);
    }
}
