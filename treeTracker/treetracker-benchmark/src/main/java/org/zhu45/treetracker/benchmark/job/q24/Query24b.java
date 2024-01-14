package org.zhu45.treetracker.benchmark.job.q24;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query24b
        extends Query
{
    public Query24b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q24b);
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q24b);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q24b);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(JOBQueries.Q24b);
        MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q24b);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q24b, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q24b);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q24b, null);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q24b);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q24b, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q24b, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(castInfoNode, movieInfoNode),
                asEdge(movieInfoNode, infoTypeNode),
                asEdge(castInfoNode, akaNameNode),
                asEdge(akaNameNode, nameNode),
                asEdge(castInfoNode, roleTypeNode),
                asEdge(castInfoNode, charNameNode),
                asEdge(castInfoNode, movieCompaniesNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(castInfoNode, titleNode),
                asEdge(castInfoNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode)), castInfoNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(castInfoNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                infoTypeNode.getSchemaTableName(),
                akaNameNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                roleTypeNode.getSchemaTableName(),
                charNameNode.getSchemaTableName(),
                movieCompaniesNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName()));
        return pair;
    }
}
