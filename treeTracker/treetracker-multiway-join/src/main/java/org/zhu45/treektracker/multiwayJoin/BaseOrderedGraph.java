package org.zhu45.treektracker.multiwayJoin;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treetracker.common.DirectedEdge;
import org.zhu45.treetracker.common.Domain;
import org.zhu45.treetracker.common.Node;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.common.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;

/**
 * This represents ordered constraint graph (e.g., Graph class)
 */
public class BaseOrderedGraph<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
        implements OrderedGraph<T, V, D>
{
    // index of each node represents the order of each node get visited.
    protected List<T> traversalList;
    // key: node Val: a list of nodes that are children of key
    // e.g., A -> B will be key: A Val: B
    protected HashMap<T, List<T>> children;
    // key: node Val: a list of nodes that are parent of key
    // e.g., A -> B will be key: B Val: A
    private HashMap<T, List<T>> parent;
    // A node that serves as the root of the ordered constraint graph represented by `nodes`
    private T root;
    // key: the depth of the node, val: the list of nodes that have the same depth
    private LinkedHashMap<Integer, List<T>> depth = new LinkedHashMap<>();

    public BaseOrderedGraph(T root,
                            HashMap<T, List<T>> children,
                            HashMap<T, List<T>> parent,
                            List<T> traversalList)
    {
        constructorHelper(root, children, parent, traversalList);
    }

    private void constructorHelper(T root,
                                   HashMap<T, List<T>> children,
                                   HashMap<T, List<T>> parent,
                                   List<T> traversalList)
    {
        checkArgument(children.size() == traversalList.size(), "every node in the graph should appear in the children map");
        checkArgument(parent.size() == traversalList.size(), "every node in the graph should appear in the parent map");
        this.root = root;
        this.children = children;
        this.parent = parent;
        this.traversalList = traversalList;
        generateDepth(root, 0);
        setNodeType();
    }

    /**
     * Perform a deep copy of the given instance (note, in general, the copy constructor provides a
     * shallow copy, which is different from this implementation)
     */
    public BaseOrderedGraph(BaseOrderedGraph<T, V, D> graph)
    {
        this.traversalList = SerializationUtils.clone(new ArrayList<>(graph.getTraversalList()));

        HashMap<T, T> oldNodeToNewNode = new HashMap<>();
        for (T oldBaseNode : graph.getChildren().keySet()) {
            for (T newBaseNode : traversalList) {
                if (oldBaseNode.hashCode() == newBaseNode.hashCode()) {
                    oldNodeToNewNode.put(oldBaseNode, newBaseNode);
                    break;
                }
            }
        }

        this.children = new HashMap<>();
        for (T parentBaseNode : graph.getChildren().keySet()) {
            List<T> childBaseNodes = graph.getChildren().get(parentBaseNode);
            this.children.put(oldNodeToNewNode.get(parentBaseNode), new ArrayList<>());
            for (T childBaseNode : childBaseNodes) {
                this.children.get(oldNodeToNewNode.get(parentBaseNode)).add(oldNodeToNewNode.get(childBaseNode));
            }
        }

        this.root = traversalList.get(0);
        checkState(this.root.getNodeType() == NodeType.Root);
        this.parent = new HashMap<>();
        for (T parentBaseNode : children.keySet()) {
            List<T> childBaseNodes = children.get(parentBaseNode);
            for (T childBaseNode : childBaseNodes) {
                parent.computeIfAbsent(childBaseNode, k -> new ArrayList<>());
                parent.get(childBaseNode).add(parentBaseNode);
            }
        }
        parent.put(root, new ArrayList<>());
        checkState(validate());
        this.depth = new LinkedHashMap<>();
        for (Integer dep : graph.getDepth().keySet()) {
            this.depth.put(dep, new ArrayList<>());
            for (T baseNode : graph.getDepth().get(dep)) {
                this.depth.get(dep).add(oldNodeToNewNode.get(baseNode));
            }
        }
    }

    public BaseOrderedGraph(T root, List<DirectedEdge<T, V, D>> directedEdges, List<T> traversalList)
    {
        HashMap<T, List<T>> children = new HashMap<>();
        HashMap<T, List<T>> parent = new HashMap<>();
        for (DirectedEdge<T, V, D> edge : directedEdges) {
            T head = edge.getHead();
            T tail = edge.getTail();
            children.computeIfAbsent(head, k -> new ArrayList<>());
            children.get(head).add(tail);
            parent.computeIfAbsent(tail, k -> new ArrayList<>());
            parent.get(tail).add(head);
        }
        for (T baseNode : traversalList) {
            if (!children.containsKey(baseNode)) {
                children.put(baseNode, new ArrayList<>());
            }
            else if (!parent.containsKey(baseNode)) {
                parent.put(baseNode, new ArrayList<>());
            }
        }
        constructorHelper(root, children, parent, traversalList);
    }

    /**
     * Calculate the width of ordered constraint graph
     *
     * @return width
     */
    @Override
    public int getWidth()
    {
        int width = 0;
        for (T baseNode : parent.keySet()) {
            width = max(width, parent.get(baseNode).size());
        }
        return width;
    }

    /**
     * checks whether the ordered graph is a tree
     */
    @Override
    public boolean isOgATree()
    {
        return getWidth() <= 1;
    }

    @Override
    public T getRoot()
    {
        return this.root;
    }

    @Override
    public HashMap<T, List<T>> getParent()
    {
        return this.parent;
    }

    @Override
    public List<T> getTraversalList()
    {
        return this.traversalList;
    }

    @Override
    public HashMap<T, List<T>> getChildren()
    {
        return this.children;
    }

    protected void generateDepth(T root, int depth)
    {
        List<T> childrenBaseNodes = children.get(root);
        if (this.depth.containsKey(depth)) {
            this.depth.get(depth).add(root);
        }
        else {
            this.depth.put(depth, new ArrayList<>(Arrays.asList(root)));
        }
        checkState(childrenBaseNodes != null, root + " doesn't exist in children hashmap");
        for (T childBaseNode : childrenBaseNodes) {
            generateDepth(childBaseNode, depth + 1);
        }
    }

    private void setNodeType()
    {
        if (root.getNodeType() == NodeType.None) {
            root.setNodeType(NodeType.Root);
        }
        for (T baseNode : children.keySet()) {
            if (baseNode.getNodeType() == NodeType.None) {
                if (children.get(baseNode).isEmpty()) {
                    baseNode.setNodeType(NodeType.Leaf);
                }
                else {
                    baseNode.setNodeType(NodeType.Internal);
                }
            }
        }
    }

    public LinkedHashMap<Integer, List<T>> getDepth()
    {
        return depth;
    }

    /**
     * Return the number of nodes between the start of the node (including) and end of the node (excluding) in pre-order traversal.
     * End node has to be in the subtree of the start node.
     */
    public int getPathLength(T start, T end)
    {
        return getPathLengthHelper(start, start, end).getKey();
    }

    private Pair<Integer, Boolean> getPathLengthHelper(T originalStart, T start, T end)
    {
        int length = 0;
        if (start.equals(end)) {
            return Pair.of(length, true);
        }
        for (T child : children.get(start)) {
            if (child.equals(end)) {
                return Pair.of(length + 1, true);
            }
            else {
                length += 1;
                Pair<Integer, Boolean> pair = getPathLengthHelper(originalStart, child, end);
                length += pair.getKey();
                if (pair.getValue()) {
                    return Pair.of(length, true);
                }
            }
        }
        if (!start.equals(originalStart)) {
            return Pair.of(length, false);
        }
        throw new IllegalArgumentException(String.format("Given end node: %s is not in the subtree(s) of the start node: %s", end, originalStart));
    }

    /**
     * Check if the current graph satisfies the structure condition, i.e.,
     * traversalList, children, parent all have the same size.
     *
     * @return true if the condition is satisfied; false otherwise.
     */
    public boolean validate()
    {
        return children.size() == traversalList.size() &&
                parent.size() == traversalList.size();
    }

    public List<T> topologicalSort()
    {
        Queue<T> s = new LinkedList<>();
        List<T> l = new ArrayList<>();
        Map<T, Set<T>> toBeRemoved = new HashMap<>();
        for (T node : traversalList) {
            if (node.getNodeType() == NodeType.Root) {
                s.add(node);
            }
        }
        while (!s.isEmpty()) {
            T node = s.poll();
            l.add(node);
            for (T child : children.get(node)) {
                toBeRemoved.computeIfAbsent(child, k -> new HashSet<>());
                toBeRemoved.get(child).add(node);
                HashSet<T> set = new HashSet<>(parent.get(child));
                if (toBeRemoved.get(child).equals(set)) {
                    s.add(child);
                }
            }
        }
        return l;
    }

    public boolean isOgADag()
    {
        return topologicalSort().size() == traversalList.size();
    }
}
