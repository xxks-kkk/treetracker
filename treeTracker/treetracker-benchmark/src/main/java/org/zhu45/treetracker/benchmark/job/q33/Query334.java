package org.zhu45.treetracker.benchmark.job.q33;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getLinkTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieLinkInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query334
        extends Query
{
    public Query334(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieLinkNode = getMovieLinkInt();
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q33, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q33, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q33, null);
        MultiwayJoinNode linkTypeNode = getLinkTypeInt(JOBQueries.Q33);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q33, null);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q33, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q33, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, movieInfoIdxNode),
                asEdge(movieInfoIdxNode, infoTypeNode),
                asEdge(movieCompaniesNode, movieLinkNode),
                asEdge(movieLinkNode, linkTypeNode),
                asEdge(movieCompaniesNode, titleNode),
                asEdge(titleNode, kindTypeNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName(),
                        movieInfoIdxNode.getSchemaTableName(),
                        infoTypeNode.getSchemaTableName(),
                        movieLinkNode.getSchemaTableName(),
                        linkTypeNode.getSchemaTableName(),
                        titleNode.getSchemaTableName(),
                        kindTypeNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
