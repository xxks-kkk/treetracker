package org.zhu45.treetracker.benchmark.job.q27;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompCastTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompleteCastInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getLinkTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieLinkInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query27
        extends Query
{
    public Query27(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode completeCastNode = getCompleteCastInt();
        MultiwayJoinNode compCastTypeNode = getCompCastTypeInt(JOBQueries.Q27, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q27, null);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q27, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q27);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q27, null);
        MultiwayJoinNode movieLinkNode = getMovieLinkInt();
        MultiwayJoinNode linkTypeNode = getLinkTypeInt(JOBQueries.Q27);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q27);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(titleNode, movieLinkNode),
                asEdge(movieLinkNode, linkTypeNode),
                asEdge(movieLinkNode, movieInfoNode),
                asEdge(titleNode, movieKeywordNode),
                asEdge(movieKeywordNode, completeCastNode),
                asEdge(completeCastNode, movieCompaniesNode),
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(completeCastNode, compCastTypeNode)), titleNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(titleNode.getSchemaTableName(),
                        movieLinkNode.getSchemaTableName(),
                        linkTypeNode.getSchemaTableName(),
                        movieInfoNode.getSchemaTableName(),
                        movieKeywordNode.getSchemaTableName(),
                        completeCastNode.getSchemaTableName(),
                        movieCompaniesNode.getSchemaTableName(),
                        companyTypeNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName(),
                        compCastTypeNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
