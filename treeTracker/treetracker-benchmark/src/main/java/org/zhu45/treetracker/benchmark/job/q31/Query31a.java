package org.zhu45.treetracker.benchmark.job.q31;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.QueryGraphEdge;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;

public class Query31a
        extends Query
{
    public Query31a(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q31a);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q31a);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q31a);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q31a);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q31a, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q31a, TableInstanceId.TWO);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q31a, null);
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q31a, TableInstanceId.TWO);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q31a, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q31a, null);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(castInfoNode, nameNode),
                asQueryGraphEdge(castInfoNode, titleNode),
                asQueryGraphEdge(castInfoNode, movieCompaniesNode),
                asQueryGraphEdge(movieCompaniesNode, companyNameNode),
                asQueryGraphEdge(castInfoNode, movieInfoNode),
                asQueryGraphEdge(movieInfoNode, movieKeywordNode),
                asQueryGraphEdge(movieKeywordNode, keywordNode),
                asQueryGraphEdge(movieInfoNode, movieInfoIdxNode2),
                asQueryGraphEdge(movieInfoIdxNode2, infoTypeNode1),
                asQueryGraphEdge(infoTypeNode1, infoTypeNode2));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, castInfoNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(castInfoNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                movieCompaniesNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                movieInfoIdxNode2.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName()));
        return pair;
    }
}
