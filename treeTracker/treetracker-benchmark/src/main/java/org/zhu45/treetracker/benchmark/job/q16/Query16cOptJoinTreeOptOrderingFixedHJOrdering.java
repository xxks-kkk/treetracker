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

import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Query16cOptJoinTreeOptOrderingFixedHJOrdering
        extends Query
{
    public Query16cOptJoinTreeOptOrderingFixedHJOrdering(JoinFragmentContext context)
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

        List<MultiwayJoinNode> traversalList = List.of(movieCompaniesNode, companyNameNode, titleNode, movieKeywordNode, keywordNode, castInfoNode, nameNode, akaNameNode);

        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(movieCompaniesNode, Arrays.asList(
                asDirectedEdge(movieCompaniesNode, companyNameNode),
                asDirectedEdge(movieCompaniesNode, titleNode),
                asDirectedEdge(movieCompaniesNode, movieKeywordNode),
                asDirectedEdge(movieKeywordNode, keywordNode),
                asDirectedEdge(movieCompaniesNode, castInfoNode),
                asDirectedEdge(castInfoNode, nameNode),
                asDirectedEdge(castInfoNode, akaNameNode)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                akaNameNode.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
