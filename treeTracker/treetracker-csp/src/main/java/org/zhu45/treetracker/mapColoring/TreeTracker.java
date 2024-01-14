package org.zhu45.treetracker.mapColoring;

import org.zhu45.treektracker.multiwayJoin.OrderedGraph;
import org.zhu45.treektracker.multiwayJoin.TraversalStrategy;
import org.zhu45.treetracker.common.Constraints;
import org.zhu45.treetracker.common.Domain;
import org.zhu45.treetracker.common.Node;
import org.zhu45.treetracker.common.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TreeTracker<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>, G extends OrderedGraph<T, V, D>>
{
    // key: node val: node's goodList
    private final HashMap<T, List<V>> nodeGoodLists;
    private final G og;
    private final List<T> ordering;
    // A list of skipped variables according to TT-2 Line 13, 20
    private final List<T> skipped;
    // A list of iterators with each iterator corresponding to a node
    // in the `ordering`
    private final List<Iterator<V>> iteratorList;
    private final Constraints<T, V, D> constraints;

    public TreeTracker(TraversalStrategy<T, V, D, G> strategy, Constraints<T, V, D> constraints)
    {
        this.nodeGoodLists = new HashMap<>();
        this.og = strategy.traversal();
        this.ordering = og.getTraversalList();
        this.skipped = new ArrayList<>();
        this.iteratorList = new ArrayList<>();
        this.constraints = constraints;
    }

    public G getOrderedGraph()
    {
        return this.og;
    }

    /**
     * TreeTracker-2 algorithm
     * <p>
     * Based on Fig.5 of
     * Bayardo Jr, Roberto J., and Daniel P. Miranker.
     * "An optimal backtrack algorithm for tree-structured constraint satisfaction
     * problems."
     * Artificial Intelligence 71.1 (1994): 159-181.
     * <p>
     * NOTE: there are definitely many bugs populated in the current implementation.
     * We focus effort on TT-J,
     * which might provide pointers on which part of implementation is wrong. We may
     * need to construct some random
     * graph generator to make the implementation robust. For now, this
     * implementation serves the purpose of providing
     * an understanding of TT2 algorithm.
     *
     * @return
     */
    public List<T> treeTracker2()
    {
        if (!this.og.isOgATree()) {
            // invariant: a node has only one parent, which is required for tree-structured
            // CSP solving algorithm
            throw new IllegalArgumentException("constraint graph has width > 1");
        }
        HashMap<T, Integer> nodeToPos = new HashMap<>();
        for (int k = 0; k < this.ordering.size(); ++k) {
            T baseNode = this.ordering.get(k);
            this.iteratorList.add(baseNode.getDomain().iterator());
            nodeToPos.put(baseNode, k);
            this.nodeGoodLists.put(baseNode, new LinkedList<>());
        }
        Integer i = 0;
        while (true) {
            T currBaseNode = this.ordering.get(i);
            T parent = (currBaseNode.equals(this.og.getRoot())) ? currBaseNode
                    : this.og.getParent().get(currBaseNode).get(0);
            if (!iteratorList.get(i).hasNext()) {
                if (i == 0) {
                    return null;
                }
                else {
                    i = nodeToPos.get(parent);
                    this.nodeGoodLists.get(parent).remove(this.nodeGoodLists.get(parent).size() - 1);
                }
            }
            else {
                V v = iteratorList.get(i).next();
                // TODO: question: when comes to backtrack in TT2, should we only need to check
                // constraint between
                // node and its parent rather than now: checking all constraints involving the
                // node?
                // "Instantiated" definition from paper: A variable is said to be instantiated
                // if it is assigned to
                // a domain value which satisfies all constraints defined over it and the
                // variables previous to it
                // along the order. Looks like this definition doesn't care about the variables
                // after it?
                // It is ok to check one constraint (e.g., one between currBaseNode and parent)
                // because in the tree-structured
                // ordered CSP, each node has exactly one parent.
                if (this.constraints.instantiate(currBaseNode, parent, v)) {
                    if (i == this.ordering.size() - 1) {
                        for (T baseNode : skipped) {
                            boolean isSucceed = this.constraints.instantiate(baseNode,
                                    this.nodeGoodLists.get(baseNode));
                            assertTrue(isSucceed);
                        }
                        return this.ordering; // this.og is modified with assignment as well
                    }
                    else {
                        this.ordering.get(i).getDomain().remove(v);
                        this.nodeGoodLists.get(currBaseNode).add(v);
                        i++;
                        tryGoodList(i);
                    }
                }
            }
        }
    }

    private void tryGoodList(Integer i)
    {
        T currBaseNode = this.ordering.get(i);
        T parent = this.og.getParent().get(currBaseNode).get(0);
        for (V v : this.nodeGoodLists.get(currBaseNode)) {
            // This instantiation has the same question with the previous one
            if (this.constraints.instantiate(currBaseNode, parent, v)) {
                for (int k = i + 1; k < this.ordering.size(); ++k) {
                    if (!this.og.getParent().get(this.ordering.get(k)).contains(currBaseNode)) {
                        i = k;
                    }
                    else {
                        this.skipped.add(this.ordering.get(k));
                    }
                }
                tryGoodList(i);
                return;
            }
        }
        this.iteratorList.set(i, ordering.get(i).getDomain().iterator());
    }
}
