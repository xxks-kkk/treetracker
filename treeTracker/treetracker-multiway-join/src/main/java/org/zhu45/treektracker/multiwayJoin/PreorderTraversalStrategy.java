package org.zhu45.treektracker.multiwayJoin;

import org.zhu45.treetracker.common.Domain;
import org.zhu45.treetracker.common.Node;
import org.zhu45.treetracker.common.Value;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PreorderTraversalStrategy<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>, G extends OrderedGraph<T, V, D>>
        implements TraversalStrategy<T, V, D, G>
{
    // index of each node represents the order of each node get visited.
    private final List<T> preorderTraversalList;
    // key: node Val: a list of nodes that are children of key
    // e.g., A -> B will be key: A Val: B
    private final HashMap<T, List<T>> children;
    // key: node Val: a list of nodes that are parent of key
    // e.g., A -> B will be key: B Val: A
    private final HashMap<T, List<T>> parent;
    // A node that serves as the root of the ordered constraint graph represented by `nodes`
    private final T root;
    private final Class<G> orderedGraphClazz;

    public PreorderTraversalStrategy(T root, Class<G> orderedGraphClazz)
    {
        // We don't require to put in unOrderedGraph, but
        // we assume unorderedGraph has been constructed when calling this constructor
        // and the root is one of the nodes in the graph. Otherwise, the created orderedGraph
        // is just the root itself.
        this.root = root;
        this.children = new HashMap<>();
        this.parent = new HashMap<>();
        this.preorderTraversalList = new ArrayList<>();
        this.orderedGraphClazz = orderedGraphClazz;
    }

    @Override
    public G traversal()
    {
        Set<T> visited = new HashSet<>();
        visited.add(root);
        this.preorderTraversalList.add(root);
        children.put(root, root.getConnected());
        for (T baseNode : children.get(root)) {
            boolean isNotVisited = visited.add(baseNode);
            if (isNotVisited) {
                preorderTraversalHelper(baseNode, visited);
            }
        }
        // create parent hashmap based on children hashmap
        for (T baseNode : this.children.keySet()) {
            for (T child : this.children.get(baseNode)) {
                if (!this.parent.containsKey(child)) {
                    this.parent.put(child, new ArrayList<>());
                }
                this.parent.get(child).add(baseNode);
            }
            if (!this.parent.containsKey(baseNode)) {
                this.parent.put(baseNode, new ArrayList<>());
            }
        }
        try {
            Constructor constructor = orderedGraphClazz.getConstructor(root.getClass(), HashMap.class, HashMap.class, List.class);
            return (G) constructor.newInstance(this.root, this.children, this.parent, this.preorderTraversalList);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void preorderTraversalHelper(T root, Set<T> visited)
    {
        this.preorderTraversalList.add(root);
        List<T> children = root.getConnected();
        if (!this.children.containsKey(root)) {
            this.children.put(root, new ArrayList<>());
        }
        if (!children.isEmpty()) {
            for (T baseNode : children) {
                boolean isNotVisited = visited.add(baseNode);
                if (isNotVisited) {
                    this.children.get(root).add(baseNode);
                    preorderTraversalHelper(baseNode, visited);
                }
            }
        }
    }
}
