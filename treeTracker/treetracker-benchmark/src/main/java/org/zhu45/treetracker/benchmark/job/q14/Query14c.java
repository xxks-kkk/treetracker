package org.zhu45.treetracker.benchmark.job.q14;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query14c
        extends Query
{
    public Query14c(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q14c);
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q14c, TableInstanceId.TWO);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q14c, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q14c, TableInstanceId.TWO);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q14c, null);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q14c, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q14c);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieInfoNode, movieInfoIdxNode2),
                asEdge(movieInfoIdxNode2, titleNode),
                asEdge(movieInfoIdxNode2, infoTypeNode1),
                asEdge(infoTypeNode1, infoTypeNode2),
                asEdge(titleNode, movieKeywordNode),
                asEdge(titleNode, kindTypeNode),
                asEdge(movieKeywordNode, keywordNode)), movieInfoNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(movieInfoNode.getSchemaTableName(),
                movieInfoIdxNode2.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                kindTypeNode.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName()));
        return pair;
    }
}
