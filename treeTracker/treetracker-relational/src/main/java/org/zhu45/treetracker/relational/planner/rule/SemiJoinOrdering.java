package org.zhu45.treetracker.relational.planner.rule;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Semijoin ordering for the bottom-up pass of the full reducer. Note
 * that semijoin ordering only matters in the bottom-up pass of the full reducer,
 * top-down pass doesn't because by the definition of tree, each node only has one parent.
 */
public class SemiJoinOrdering
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(SemiJoinOrdering.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    // The bottom-up pass of the join tree. Pair<MultiwayJoinNode, MultiwayJoinNode> is assumed to be Pair<Parent,Child>.
    private final List<Pair<MultiwayJoinNode, MultiwayJoinNode>> bottomUpPass;
    @JsonIgnore
    // Join tree that the full reducer is built from
    private final MultiwayJoinOrderedGraph joinTree;

    public SemiJoinOrdering(List<Pair<MultiwayJoinNode, MultiwayJoinNode>> bottomUpPass,
                            MultiwayJoinOrderedGraph joinTree)
    {
        this.joinTree = joinTree;
        this.bottomUpPass = bottomUpPass;
    }

    public static void integrityCheck(SemiJoinOrdering semiJoinOrdering)
    {
        checkState(semiJoinOrdering.bottomUpPass.size() == semiJoinOrdering.joinTree.getTraversalList().size() - 1,
                "full reducer contains k-1 semijoins in the bottom-up pass where k is the number of relations");
        for (Pair<MultiwayJoinNode, MultiwayJoinNode> pair : semiJoinOrdering.bottomUpPass) {
            checkState(semiJoinOrdering.joinTree.getChildren().get(pair.getLeft()).contains(pair.getRight()),
                    pair.getLeft() + "is not parent of " + pair.getRight());
        }
    }

    public List<Pair<MultiwayJoinNode, MultiwayJoinNode>> getBottomUpPass()
    {
        return bottomUpPass;
    }

    public MultiwayJoinOrderedGraph getJoinTree()
    {
        return joinTree;
    }

    public void append(SemiJoinOrdering semiJoinOrdering)
    {
        checkArgument(semiJoinOrdering.getJoinTree() == joinTree,
                "input semijoinOrdering " + semiJoinOrdering + "doesn't reference the same join tree");
        bottomUpPass.addAll(semiJoinOrdering.bottomUpPass);
    }

    public void insertAtHead(SemiJoinOrdering semiJoinOrdering)
    {
        checkArgument(semiJoinOrdering.getJoinTree() == joinTree,
                "input semijoinOrdering " + semiJoinOrdering + "doesn't reference the same join tree");
        bottomUpPass.addAll(0, semiJoinOrdering.bottomUpPass);
    }

    @Override
    public String toString()
    {
        return bottomUpPass.toString();
    }
}
