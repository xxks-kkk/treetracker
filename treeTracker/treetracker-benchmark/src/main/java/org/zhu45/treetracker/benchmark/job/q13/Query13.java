package org.zhu45.treetracker.benchmark.job.q13;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.QueryGraphEdge;
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
import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;

public class Query13
        extends Query
{
    public Query13(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q13, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q13, null);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q13, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q13, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q13);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q13);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q13, null);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q13, null);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(movieCompaniesNode, titleNode),
                asQueryGraphEdge(titleNode, kindTypeNode),
                asQueryGraphEdge(movieCompaniesNode, companyNameNode),
                asQueryGraphEdge(movieCompaniesNode, companyTypeNode),
                asQueryGraphEdge(movieCompaniesNode, movieInfoNode),
                asQueryGraphEdge(movieInfoNode, infoTypeNode),
                asQueryGraphEdge(movieInfoNode, movieInfoIdxNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                        titleNode.getSchemaTableName(),
                        kindTypeNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName(),
                        companyTypeNode.getSchemaTableName(),
                        movieInfoNode.getSchemaTableName(),
                        infoTypeNode.getSchemaTableName(),
                        movieInfoIdxNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
