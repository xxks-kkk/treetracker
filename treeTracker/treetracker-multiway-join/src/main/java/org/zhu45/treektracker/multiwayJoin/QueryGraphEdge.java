package org.zhu45.treektracker.multiwayJoin;

import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.row.Row;

public class QueryGraphEdge
        extends Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>
{
    public QueryGraphEdge(MultiwayJoinNode tail, MultiwayJoinNode head)
    {
        super(tail, head);
    }

    public static QueryGraphEdge asQueryGraphEdge(MultiwayJoinNode tail, MultiwayJoinNode head)
    {
        return new QueryGraphEdge(tail, head);
    }
}
