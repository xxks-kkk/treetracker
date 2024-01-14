package org.zhu45.treetracker.benchmark.job.patterns;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;

public class MovieCompaniesPattern1
        extends Query
{
    public MovieCompaniesPattern1(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.NOPREDICATE, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.NOPREDICATE);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.NOPREDICATE, null);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(movieCompaniesNode, companyTypeNode),
                asQueryGraphEdge(movieCompaniesNode, companyNameNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, companyTypeNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(companyTypeNode.getSchemaTableName(),
                        movieCompaniesNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
