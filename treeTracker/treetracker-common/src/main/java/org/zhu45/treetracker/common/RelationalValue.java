package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.type.Type;

/**
 * Represents an entry inside a relation (i.e., table)
 */
public interface RelationalValue
{
    Type getType();
}
