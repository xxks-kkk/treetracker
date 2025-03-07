package org.zhu45.treektracker.multiwayJoin;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.Sets;
import org.zhu45.treetracker.common.DirectedEdge;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.JdbcSupplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MultiwayJoinOrderedGraph
        extends BaseOrderedGraph<MultiwayJoinNode, Row, MultiwayJoinDomain>
{
    private static final LoggerProvider.TreeTrackerLogger log = LoggerProvider.getLogger(MultiwayJoinOrderedGraph.class);

    public MultiwayJoinOrderedGraph(MultiwayJoinNode root,
                                    HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> children,
                                    HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> parent,
                                    List<MultiwayJoinNode> traversalList)
    {
        super(root, children, parent, traversalList);
    }

    public MultiwayJoinOrderedGraph(MultiwayJoinOrderedGraph orderedGraph)
    {
        super(orderedGraph);
    }

    public MultiwayJoinOrderedGraph(MultiwayJoinNode root, List<DirectedEdge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeList, List<MultiwayJoinNode> traversalList)
    {
        super(root, edgeList, traversalList);
    }

    @Override
    @JsonValue
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        buildGraphToText(0, getRoot(), builder);
        return builder.toString();
    }

    /**
     * For each leaf, the function assumes it appears in the children map and is associated with
     * an empty list. Accordingly, for the root, the function assumes it appears in the parent map as well
     * and is associated with an empty list.
     */
    private void buildGraphToText(int depth, MultiwayJoinNode root, StringBuilder builder)
    {
        if (!children.containsKey(root)) {
            return;
        }
        for (int i = 0; i < depth; ++i) {
            builder.append("|");
        }
        List<String> attributes = root.getAttributes() == null ?
                JdbcSupplier.postgresJdbcClientSupplier.get().getAttributes(root.getSchemaTableName()) :
                root.getAttributes();
        builder.append(root)
                .append("(")
                .append(String.join(",", attributes))
                .append(")");
        for (MultiwayJoinNode node : children.get(root)) {
            builder.append("\n");
            buildGraphToText(depth + 1, node, builder);
        }
    }

    public static List<SchemaTableName> getSchemaTableNameList(MultiwayJoinOrderedGraph orderedGraph)
    {
        return orderedGraph.getTraversalList()
                .stream()
                .map(node -> requireNonNull(node.getSchemaTableName()))
                .collect(Collectors.toList());
    }

    public MultiwayJoinGraph obtainQueryGraph()
    {
        List<QueryGraphEdge> edgeList = new ArrayList<>();
        List<MultiwayJoinNode> nodesCpy = getTraversalList().stream().map(MultiwayJoinNode::new).collect(Collectors.toList());
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> connected = new HashMap<>();
        for (int i = 0; i < nodesCpy.size(); ++i) {
            MultiwayJoinNode node1 = nodesCpy.get(i);
            connected.computeIfAbsent(node1, k -> new ArrayList<>());
            for (int j = 0; j < nodesCpy.size(); ++j) {
                MultiwayJoinNode node2 = nodesCpy.get(j);
                if (!node1.equals(node2) &&
                        !connected.get(node1).contains(node2) &&
                        !Sets.intersection(Sets.newHashSet(node1.getAttributes()), Sets.newHashSet(node2.getAttributes())).isEmpty()) {
                    edgeList.add(QueryGraphEdge.asQueryGraphEdge(node1, node2));
                    connected.computeIfAbsent(node1, k -> new ArrayList<>());
                    connected.computeIfAbsent(node2, k -> new ArrayList<>());
                    connected.get(node1).add(node2);
                    connected.get(node2).add(node1);
                }
            }
        }
        for (MultiwayJoinNode node : nodesCpy) {
            node.setConnected(connected.get(node));
        }
        return new MultiwayJoinGraph(edgeList);
    }

    public void setDepth()
    {
        this.getDepth().clear();
        generateDepth(this.getRoot(), 0);
    }

    public void setTraversalList(List<MultiwayJoinNode> traversalList)
    {
        this.traversalList = traversalList;
    }
}
