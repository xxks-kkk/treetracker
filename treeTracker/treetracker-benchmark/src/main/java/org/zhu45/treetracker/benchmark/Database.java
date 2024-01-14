package org.zhu45.treetracker.benchmark;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;

import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class Database
{
    private Database()
    {
    }

    public static MultiwayJoinNode getTableNode(SchemaTableName schemaTableName)
    {
        return MultiwayJoinNode.getTableNode(schemaTableName, duckDBJdbcClientSupplier.get());
    }
}
