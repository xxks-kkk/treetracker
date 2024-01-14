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

import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;

public class TupleFetchQuery
        extends Query
{
    public TupleFetchQuery(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q8c, null);

        MultiwayJoinGraph g = new MultiwayJoinGraph(titleNode);
        PreorderTraversalStrategy<MultiwayJoinNode, Row, MultiwayJoinDomain, MultiwayJoinOrderedGraph> strategy =
                new PreorderTraversalStrategy<>(titleNode, MultiwayJoinOrderedGraph.class);
        MultiwayJoinOrderedGraph orderedGraph = strategy.traversal();

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
