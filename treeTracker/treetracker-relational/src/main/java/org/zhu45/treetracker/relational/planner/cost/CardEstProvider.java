package org.zhu45.treetracker.relational.planner.cost;

import org.zhu45.treetracker.common.SchemaTableName;

import java.util.Set;

/**
 * The cardinality estimate provider interface
 */
public interface CardEstProvider
{
    // get natural join output size (can be estimation depending on the interface impl)
    // of the given set of relations.
    default CardEstReturn getSize(Set<SchemaTableName> schemaTableNames, CardEstContext context)
    {
        throw new UnsupportedOperationException();
    }

    CardEstType getCardEstType();

    boolean isUseTrueCard();

    default CardEstProviderStatistics getStatistics()
    {
        throw new UnsupportedOperationException();
    }
}
