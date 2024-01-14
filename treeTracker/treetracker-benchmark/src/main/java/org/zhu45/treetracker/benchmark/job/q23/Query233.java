package org.zhu45.treetracker.benchmark.job.q23;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query233
        extends Query
{
    public Query233(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode completeCastNode = getCompleteCastInt();
        MultiwayJoinNode compCastTypeNode = getCompCastTypeInt(JOBQueries.Q23, null);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q23, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q23);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q23, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q23);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q23);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q23, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q23, null);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q23, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(completeCastNode, compCastTypeNode),
                asEdge(completeCastNode, movieCompaniesNode),
                asEdge(movieCompaniesNode, titleNode),
                asEdge(titleNode, kindTypeNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, movieInfoNode),
                asEdge(movieInfoNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(movieInfoNode, infoTypeNode)), completeCastNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(completeCastNode.getSchemaTableName(),
                        compCastTypeNode.getSchemaTableName(),
                        movieCompaniesNode.getSchemaTableName(),
                        titleNode.getSchemaTableName(),
                        kindTypeNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName(),
                        companyTypeNode.getSchemaTableName(),
                        movieInfoNode.getSchemaTableName(),
                        movieKeywordNode.getSchemaTableName(),
                        keywordNode.getSchemaTableName(),
                        infoTypeNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
