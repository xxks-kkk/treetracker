package org.zhu45.treetracker.relational;

/**
 * Used internally by
 * - Identify type of logical plan node
 * - Identify type of physical operator
 */
public enum OptType
{
    join,
    table,
    hash,
    gather,
    gather_merge,
    aggregate,
    sort,
    fullReducer,
    materialize
}
