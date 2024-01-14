package org.zhu45.treektracker.multiwayJoin;

import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.Graph;
import org.zhu45.treetracker.common.row.Row;

import java.util.List;
import java.util.Objects;

/**
 * We use adjacency list representation. We take edge list as an input to
 * build our graph.
 * <p>
 * We follow the definition of query graph from:
 * <p>
 * Miranker, Daniel P., Roberto J. Bayardo, and Vasilis Samoladas.
 * "Query evaluation as constraint search; an overview of early results."
 * International Workshop on Constraint Database Systems. Springer, Berlin, Heidelberg, 1997.
 */
public class MultiwayJoinGraph
        extends Graph<MultiwayJoinNode, Row, MultiwayJoinDomain>
{
    public MultiwayJoinGraph(List<? extends Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeList)
    {
        super(edgeList);
    }

    public MultiwayJoinGraph(MultiwayJoinNode node)
    {
        super(node);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (!(o instanceof MultiwayJoinGraph)) {
            return false;
        }
        MultiwayJoinGraph c = (MultiwayJoinGraph) o;
        return c.getNodes().equals(this.getNodes());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.getNodes());
    }
}
