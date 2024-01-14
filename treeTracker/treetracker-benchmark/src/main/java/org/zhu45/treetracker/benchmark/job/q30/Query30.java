package org.zhu45.treetracker.benchmark.job.q30;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
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

public class Query30
        extends Query
{
    public Query30(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode completeCastNode = getCompleteCastInt();
        MultiwayJoinNode compCastTypeNode = getCompCastTypeInt(JOBQueries.Q30, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q30);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q30);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q30);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q30);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q30, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q30, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q30, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(completeCastNode, compCastTypeNode),
                asEdge(completeCastNode, castInfoNode),
                asEdge(castInfoNode, nameNode),
                asEdge(castInfoNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(castInfoNode, movieInfoIdxNode),
                asEdge(movieInfoIdxNode, movieInfoNode),
                asEdge(movieInfoIdxNode, infoTypeNode),
                asEdge(completeCastNode, titleNode)), completeCastNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(completeCastNode.getSchemaTableName(),
                        compCastTypeNode.getSchemaTableName(),
                        castInfoNode.getSchemaTableName(),
                        nameNode.getSchemaTableName(),
                        movieKeywordNode.getSchemaTableName(),
                        keywordNode.getSchemaTableName(),
                        movieInfoIdxNode.getSchemaTableName(),
                        movieInfoNode.getSchemaTableName(),
                        infoTypeNode.getSchemaTableName(),
                        titleNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
