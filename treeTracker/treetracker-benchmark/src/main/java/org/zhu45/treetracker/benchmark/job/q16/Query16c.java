package org.zhu45.treetracker.benchmark.job.q16;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query16c
        extends Query
{
    public Query16c(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q16c, null);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q16c, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q16c);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q16c);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q16c);
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q16c);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q16c, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(companyNameNode, movieCompaniesNode),
                asEdge(movieCompaniesNode, castInfoNode),
                asEdge(castInfoNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(movieKeywordNode, titleNode),
                asEdge(castInfoNode, nameNode),
                asEdge(nameNode, akaNameNode)), companyNameNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(companyNameNode.getSchemaTableName(),
                movieCompaniesNode.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                akaNameNode.getSchemaTableName()));
        return pair;
    }
}
