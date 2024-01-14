package org.zhu45.treetracker.relational.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.EdgeWeight;

import java.util.List;
import java.util.Objects;

/**
 * Edge weight used to construct a join tree from a given query graph
 */
public class JoinTreeEdgeWeight
        implements EdgeWeight
{
    private final ImmutableList<String> edgeLabel;

    public JoinTreeEdgeWeight(MultiwayJoinNode node1, MultiwayJoinNode node2)
    {
        // We don't keep node1 and node2 as part of the class because
        // if we do, two instances (node1 = a, node2 = b), (node1 = b, node2 = a) are
        // considered as two different JoinTreeEdgeWeight despite they are the same.
        List<String> node1attrs = node1.getAttributes();
        List<String> node2attrs = node2.getAttributes();
        edgeLabel = Sets.intersection(Sets.newHashSet(node1attrs), Sets.newHashSet(node2attrs)).immutableCopy().asList();
    }

    @Override
    public Integer getWeight()
    {
        return edgeLabel.size();
    }

    public List<String> getEdgeLabel()
    {
        return edgeLabel;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(edgeLabel);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JoinTreeEdgeWeight other = (JoinTreeEdgeWeight) obj;
        return Objects.equals(this.edgeLabel, other.edgeLabel);
    }
}
