package org.zhu45.treetracker.benchmark.job.q26;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompCastTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompleteCastInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query26a
        extends Query
{
    public Query26a(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode completeCastNode = getCompleteCastInt();
        MultiwayJoinNode compCastTypeNode1 = getCompCastTypeInt(JOBQueries.Q26a, TableInstanceId.ONE);
        MultiwayJoinNode compCastTypeNode2 = getCompCastTypeInt(JOBQueries.Q26a, TableInstanceId.TWO);
        MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q26a);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q26a, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q26a);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q26a);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q26a);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q26a, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q26a, null);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q26a, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(completeCastNode, titleNode),
                asEdge(titleNode, kindTypeNode),
                asEdge(completeCastNode, compCastTypeNode1),
                asEdge(completeCastNode, compCastTypeNode2),
                asEdge(completeCastNode, movieInfoIdxNode),
                asEdge(movieInfoIdxNode, castInfoNode),
                asEdge(castInfoNode, charNameNode),
                asEdge(castInfoNode, nameNode),
                asEdge(movieInfoIdxNode, infoTypeNode),
                asEdge(movieInfoIdxNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode)), completeCastNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(completeCastNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                kindTypeNode.getSchemaTableName(),
                compCastTypeNode1.getSchemaTableName(),
                compCastTypeNode2.getSchemaTableName(),
                movieInfoIdxNode.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                charNameNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                infoTypeNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName()));
        return pair;
    }
}
