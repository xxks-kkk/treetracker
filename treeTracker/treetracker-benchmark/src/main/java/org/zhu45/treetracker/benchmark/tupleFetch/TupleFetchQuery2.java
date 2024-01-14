package org.zhu45.treetracker.benchmark.tupleFetch;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.PreorderTraversalStrategy;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.List;

import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;

public class TupleFetchQuery2
        extends Query
{
    public TupleFetchQuery2(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q8c);

        MultiwayJoinGraph g = new MultiwayJoinGraph(castInfoNode);
        PreorderTraversalStrategy<MultiwayJoinNode, Row, MultiwayJoinDomain, MultiwayJoinOrderedGraph> strategy =
                new PreorderTraversalStrategy<>(castInfoNode, MultiwayJoinOrderedGraph.class);
        MultiwayJoinOrderedGraph orderedGraph = strategy.traversal();

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
