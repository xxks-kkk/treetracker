package org.zhu45.treetracker.benchmark.job.q4;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query4c
        extends Query
{
    public Query4c(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q4c, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q4c);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q4c, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q4c, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieInfoIdxNode, titleNode),
                asEdge(movieInfoIdxNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(movieInfoIdxNode, infoTypeNode)), movieInfoIdxNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(movieInfoIdxNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                infoTypeNode.getSchemaTableName()));
        return pair;
    }
}
