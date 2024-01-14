package org.zhu45.treetracker.jdbc.testing;

import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.jdbc.JdbcClient;

import java.util.List;
import java.util.Optional;

public interface Database<R extends Row>
{
    List<SchemaTableName> getRelations();

    JdbcClient getJdbcClient();

    default String getSchemaName()
    {
        throw new UnsupportedOperationException();
    }

    Optional<Long> getSeed();

    RowSet<R> getRelationRows(String relationName);

    int getNumRelations();
}
