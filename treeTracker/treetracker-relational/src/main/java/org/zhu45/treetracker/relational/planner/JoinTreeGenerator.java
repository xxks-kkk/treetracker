package org.zhu45.treetracker.relational.planner;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.CheckLabelConnectedness;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.common.SchemaTableName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph.getSchemaTableNameList;
import static org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrdering.populateSchemaTableNameAttributes;

public class JoinTreeGenerator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(JoinTreeGenerator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private HashMap<SchemaTableName, HashSet<String>> schemaTableNameAttributes;
    PlanBuildContext context;

    public JoinTreeGenerator(PlanBuildContext context)
    {
        this.context = context;
    }

    public List<MultiwayJoinOrderedGraph> generateJoinTrees(List<SchemaTableName> schemaTableNameList)
    {
        schemaTableNameAttributes = populateSchemaTableNameAttributes(schemaTableNameList, context.getJdbcClient());
        // key: attribute, vals: a list of relations that contains the given attribute
        HashMap<String, List<MultiwayJoinNode>> attributeToNodes = new HashMap<>();
        List<MultiwayJoinOrderedGraph> joinTrees = new ArrayList<>();
        MultiwayJoinOrderedGraph joinTree = createSingleNodeJoinTree(schemaTableNameList.get(0));
        MultiwayJoinNode root = joinTree.getRoot();
        List<MultiwayJoinNode> allNodesList = new ArrayList<>(List.of(root));
        allNodesList.addAll(schemaTableNameList
                .subList(1, schemaTableNameList.size())
                .stream()
                .map(schemaTableName -> new MultiwayJoinNode(schemaTableName,
                        new ArrayList<>(schemaTableNameAttributes.get(schemaTableName)),
                        new MultiwayJoinDomain()))
                .collect(Collectors.toList()));
        List<String> attributes = root.getAttributes();
        for (String attribute : attributes) {
            attributeToNodes.put(attribute, new ArrayList<>(List.of(root)));
        }
        joinTrees.add(joinTree);
        for (int i = 1; i < allNodesList.size(); ++i) {
            MultiwayJoinNode multiwayJoinNode = allNodesList.get(i);
            List<MultiwayJoinNode> nodesCanBeAttached = findNodesCanBeAttached(allNodesList.subList(0, i),
                    multiwayJoinNode, attributeToNodes);
            constructJoinTrees(joinTrees, nodesCanBeAttached, multiwayJoinNode);
        }
        joinTrees.forEach(tree -> {
            checkState(tree.validate());
            fillNodeConntectedness(tree);
            fillNodeType(tree);
            if (Switches.DEBUG) {
                checkState(new CheckLabelConnectedness(tree.getTraversalList(), context.getJdbcClient()).check(),
                        tree + "\nis not a join tree");
            }
            // enforce the traversalList is the same as the input ordering
            checkState(getSchemaTableNameList(tree).equals(schemaTableNameList));
        });
        return joinTrees;
    }

    public MultiwayJoinOrderedGraph createSingleNodeJoinTree(SchemaTableName schemaTableName)
    {
        MultiwayJoinNode root = new MultiwayJoinNode(schemaTableName,
                new ArrayList<>(context.getCatalogGroup().getTableCatalog(schemaTableName).getAttributes()),
                new MultiwayJoinDomain());
        root.setNodeType(NodeType.Root);
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> children = new HashMap<>();
        children.put(root, new ArrayList<>());
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> parent = new HashMap<>();
        parent.put(root, new ArrayList<>());
        return new MultiwayJoinOrderedGraph(root, children, parent, new ArrayList<>(List.of(root)));
    }

    public List<MultiwayJoinOrderedGraph> createAllPossibleJoinTreesThatAttachGivenNodeToGivenTree(
            MultiwayJoinOrderedGraph joinTree, MultiwayJoinNode node)
    {
        requireNonNull(node, "node cannot be null");
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("join tree:\n" + joinTree);
            traceLogger.debug("node: " + node);
        }
        List<SchemaTableName> newOrdering = new ArrayList<>(getSchemaTableNameList(joinTree));
        newOrdering.add(node.getSchemaTableName());
        List<MultiwayJoinOrderedGraph> joinTrees = new ArrayList<>(List.of(joinTree));
        HashMap<String, List<MultiwayJoinNode>> attributeToNodes = new HashMap<>();
        for (MultiwayJoinNode existingNode : joinTree.getTraversalList()) {
            for (String attribute : existingNode.getAttributes()) {
                attributeToNodes.computeIfAbsent(attribute, k -> new ArrayList<>());
                attributeToNodes.get(attribute).add(existingNode);
            }
        }
        List<MultiwayJoinNode> nodesCanBeAttached = findNodesCanBeAttached(joinTree.getTraversalList(),
                node, attributeToNodes);
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("nodesCanBeAttached: " + nodesCanBeAttached);
        }
        constructJoinTrees(joinTrees, nodesCanBeAttached, node);
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("Join trees:\n" + Joiner.on("\n").join(joinTrees
                    .stream().map(MultiwayJoinOrderedGraph::toString).collect(Collectors.toList())));
        }
        joinTrees.forEach(tree -> {
            checkState(tree.validate());
            fillNodeConntectedness(tree);
            fillNodeType(tree);
            if (Switches.DEBUG) {
                checkState(new CheckLabelConnectedness(tree.getTraversalList(), context.getJdbcClient()).check(),
                        tree + "\nis not a join tree");
            }
            // enforce the traversalList is the same as the input ordering
            checkState(getSchemaTableNameList(tree).equals(newOrdering),
                    "tree ordering: " + tree.getTraversalList() + "\n"
                            + "newOrdering: " + newOrdering);
        });
        return joinTrees;
    }

    private List<MultiwayJoinNode> findNodesCanBeAttached(List<MultiwayJoinNode> processedBaseNodes,
                                                          MultiwayJoinNode node,
                                                          HashMap<String, List<MultiwayJoinNode>> attributeToNodes)
    {
        // find intersection of nodes from attributeToNodes that corresponds to each attribute in the given node
        // and see if its empty (shouldn't be). Need to handle the unique attribute case, i.e., the attribute only
        // appears in the given node
        List<MultiwayJoinNode> nodesCanBeAttached = new ArrayList<>();
        List<List<MultiwayJoinNode>> scanList = new ArrayList<>();

        List<String> attributes = node.getAttributes();
        for (String attribute : attributes) {
            if (attributeToNodes.containsKey(attribute)) {
                attributeToNodes.get(attribute).add(node);
            }
            else {
                attributeToNodes.put(attribute, new ArrayList<>(List.of(node)));
            }
        }

        for (String attribute : attributes) {
            List<MultiwayJoinNode> nodes = attributeToNodes.get(attribute);
            // We ignore the unique attribute, i.e., attributes appear only in the given node
            if (nodes.size() != 1) {
                scanList.add(nodes);
            }
        }
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("attributeToNodes: " + attributeToNodes);
            traceLogger.debug("scanList: " + scanList);
            traceLogger.debug("processedBaseNodes: " + processedBaseNodes);
            traceLogger.debug("node: " + node);
        }
        checkState(scanList.size() > 0, "scanList is empty.\n" +
                "attributeToNodes: " + attributeToNodes + "\n" +
                "scanList: " + scanList + "\n" +
                "processedBaseNodes: " + processedBaseNodes + "\n" +
                "node: " + node);
        List<MultiwayJoinNode> intersection = new ArrayList<>(scanList.get(0));
        for (int i = 1; i < scanList.size(); ++i) {
            intersection.retainAll(scanList.get(i));
        }
        // intersection for sure includes the given node itself
        checkState(!intersection.isEmpty() && intersection.size() != 1,
                "the given ordering violates assumption; maye the input query is cyclic\n"
                        + "processedBaseNodes: " + processedBaseNodes + "\n"
                        + "node: " + node + "\n");
        for (MultiwayJoinNode multiwayJoinNode : intersection) {
            if (processedBaseNodes.contains(multiwayJoinNode)) {
                nodesCanBeAttached.add(multiwayJoinNode);
            }
        }
        checkState(!nodesCanBeAttached.isEmpty(), "nodesCanBeAttached is empty\nprocessedBaseNodes: "
                + processedBaseNodes + "\n" + "node: " + node + "\n"
                + "attributeToNodes: " + attributeToNodes + "\n"
                + "intersection: " + intersection + "\n"
                + "attributes: " + node.getAttributes() + "\n"
                + "scanList: " + scanList);
        return nodesCanBeAttached;
    }

    private void constructJoinTrees(List<MultiwayJoinOrderedGraph> existingJoinTrees, List<MultiwayJoinNode> nodesCanBeAttached, MultiwayJoinNode nodeToBeAttached)
    {
        checkArgument(!nodesCanBeAttached.isEmpty(), "nodesCanBeAttached is empty");
        int numberOfNodesCanBeAttached = nodesCanBeAttached.size();
        int existingJoinTreeSize = existingJoinTrees.size();
        for (int j = 0; j < existingJoinTreeSize; ++j) {
            MultiwayJoinOrderedGraph joinTree = existingJoinTrees.get(j);
            MultiwayJoinOrderedGraph originalJoinTree = new MultiwayJoinOrderedGraph(joinTree);
            MultiwayJoinOrderedGraph candidateJoinTree = joinTree;
            nodeToBeAttached = new MultiwayJoinNode(nodeToBeAttached);
            nodesCanBeAttached = findNodesCanBeAttachedFromCopiedTree(nodesCanBeAttached, candidateJoinTree);
            for (int i = 0; i < numberOfNodesCanBeAttached; ++i) {
                if (i != 0) {
                    candidateJoinTree = new MultiwayJoinOrderedGraph(originalJoinTree);
                    nodesCanBeAttached = findNodesCanBeAttachedFromCopiedTree(nodesCanBeAttached, candidateJoinTree);
                    nodeToBeAttached = new MultiwayJoinNode(nodeToBeAttached);
                }
                candidateJoinTree.getTraversalList().add(nodeToBeAttached);
                MultiwayJoinNode parent = nodesCanBeAttached.get(i);
                candidateJoinTree.getChildren().computeIfAbsent(parent, k -> new ArrayList<>());
                candidateJoinTree.getChildren().get(parent).add(nodeToBeAttached);
                candidateJoinTree.getChildren().put(nodeToBeAttached, new ArrayList<>());
                candidateJoinTree.getParent().put(nodeToBeAttached, List.of(parent));
                if (i != 0) {
                    existingJoinTrees.add(candidateJoinTree);
                }
            }
        }
    }

    private List<MultiwayJoinNode> findNodesCanBeAttachedFromCopiedTree(List<MultiwayJoinNode> nodesCanBeAttached, MultiwayJoinOrderedGraph copiedTree)
    {
        List<MultiwayJoinNode> result = new ArrayList<>();
        List<MultiwayJoinNode> copiedTreeTraversalList = copiedTree.getTraversalList();
        for (MultiwayJoinNode node : nodesCanBeAttached) {
            for (MultiwayJoinNode copiedBaseNode : copiedTreeTraversalList) {
                if (node.hashCode() == copiedBaseNode.hashCode()) {
                    result.add(copiedBaseNode);
                    break;
                }
            }
        }
        return result;
    }

    /**
     * Make sure connected field of each node in the given joinTree is properly set.
     */
    private void fillNodeConntectedness(MultiwayJoinOrderedGraph joinTree)
    {
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> children = joinTree.getChildren();
        for (MultiwayJoinNode parent : children.keySet()) {
            for (MultiwayJoinNode child : children.get(parent)) {
                parent.addConnected(child);
                child.addConnected(parent);
            }
        }
    }

    /**
     * Make sure NodeType of each node in the given joinTree is properly set.
     */
    private void fillNodeType(MultiwayJoinOrderedGraph joinTree)
    {
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> children = joinTree.getChildren();
        for (MultiwayJoinNode parent : children.keySet()) {
            List<MultiwayJoinNode> childBaseNodes = children.get(parent);
            if (childBaseNodes.isEmpty()) {
                parent.setNodeType(NodeType.Leaf);
            }
            else if (parent.getNodeType() != NodeType.Root) {
                parent.setNodeType(NodeType.Internal);
            }
        }
    }
}
