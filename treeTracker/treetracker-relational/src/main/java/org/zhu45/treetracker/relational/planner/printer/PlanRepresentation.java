package org.zhu45.treetracker.relational.planner.printer;

import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeId;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PlanRepresentation
{
    private final PlanNode root;

    private final Map<PlanNodeId, NodeRepresentation> nodeInfo = new HashMap<>();

    public PlanRepresentation(PlanNode root)
    {
        this.root = requireNonNull(root, "root is null");
    }

    public NodeRepresentation getRoot()
    {
        return nodeInfo.get(root.getId());
    }

    public Optional<NodeRepresentation> getNode(PlanNodeId id)
    {
        return Optional.ofNullable(nodeInfo.get(id));
    }

    public void addNode(NodeRepresentation node)
    {
        nodeInfo.put(node.getId(), node);
    }
}
