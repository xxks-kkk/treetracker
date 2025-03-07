package org.zhu45.treektracker.multiwayJoin.testing;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.jdbc.JdbcClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Checks the label-connected property of the given graph:
 * Given a set of baseNodes with labels, the graph has the property that
 * for each label, the baseNodes containing it form a connected sub-graph using only those baseNodes.
 */
public class CheckLabelConnectedness
{
    private List<MultiwayJoinNode> baseNodes;
    private HashMap<String, List<MultiwayJoinNode>> attributeNodesMap;
    private JdbcClient jdbcClient;

    public CheckLabelConnectedness(List<MultiwayJoinNode> baseNodes, JdbcClient jdbcClient)
    {
        this.baseNodes = baseNodes;
        this.attributeNodesMap = new HashMap<>();
        this.jdbcClient = jdbcClient;
        buildAttributeNodesMap();
    }

    private List<String> getAttributes(MultiwayJoinNode node)
    {
        if (node.getAttributes() != null) {
            return node.getAttributes();
        }
        return jdbcClient.getAttributes(node.getSchemaTableName());
    }

    private void buildAttributeNodesMap()
    {
        baseNodes.stream().forEach(multiwayJoinNode -> {
            List<String> attributes = getAttributes(multiwayJoinNode);
            for (String attribute : attributes) {
                if (!attributeNodesMap.containsKey(attribute)) {
                    attributeNodesMap.put(attribute, new ArrayList<>());
                }
                attributeNodesMap.get(attribute).add(multiwayJoinNode);
            }
        });
    }

    /**
     * Check whether the graph satisfies label-connected property
     *
     * @return true if it does otherwise false
     */
    public boolean check()
    {
        for (List<MultiwayJoinNode> nodes : attributeNodesMap.values()) {
            if (!isConnected(nodes)) {
                return false;
            }
        }
        return true;
    }

    private boolean isConnected(List<MultiwayJoinNode> nodesContainsSameAttribute)
    {
        if (nodesContainsSameAttribute.size() == 1) {
            return true;
        }
        for (MultiwayJoinNode node : nodesContainsSameAttribute) {
            boolean found = false;
            List<MultiwayJoinNode> connected = node.getConnected();
            for (MultiwayJoinNode connectedBaseNode : connected) {
                if (nodesContainsSameAttribute.contains(connectedBaseNode)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }
}
