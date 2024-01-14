package org.zhu45.treetracker.benchmark.job.q30;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompCastTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompleteCastInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query30c
        extends Query
{
    public Query30c(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode completeCastNode = getCompleteCastInt();
        MultiwayJoinNode compCastTypeNode1 = getCompCastTypeInt(JOBQueries.Q30c, TableInstanceId.ONE);
        MultiwayJoinNode compCastTypeNode2 = getCompCastTypeInt(JOBQueries.Q30c, TableInstanceId.TWO);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q30c);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q30c);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q30c);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q30c);
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q30c, TableInstanceId.TWO);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q30c, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q30c, TableInstanceId.TWO);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q30c, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(completeCastNode, compCastTypeNode1),
                asEdge(completeCastNode, compCastTypeNode2),
                asEdge(completeCastNode, castInfoNode),
                asEdge(castInfoNode, nameNode),
                asEdge(castInfoNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(castInfoNode, movieInfoIdxNode2),
                asEdge(movieInfoIdxNode2, movieInfoNode),
                asEdge(movieInfoIdxNode2, infoTypeNode1),
                asEdge(infoTypeNode1, infoTypeNode2),
                asEdge(completeCastNode, titleNode)), completeCastNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(completeCastNode.getSchemaTableName(),
                compCastTypeNode1.getSchemaTableName(),
                compCastTypeNode2.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                movieInfoIdxNode2.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName(),
                titleNode.getSchemaTableName()));
        return pair;
    }
}
