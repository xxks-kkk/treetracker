package org.zhu45.treetracker.benchmark.job.q10;

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
import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;

public class Query10
        extends Query
{
    public Query10(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q10, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q10);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q10, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q10);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q10, null);
        MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q10);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(JOBQueries.Q10);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asQueryGraphEdge(movieCompaniesNode, companyTypeNode),
                asQueryGraphEdge(movieCompaniesNode, castInfoNode),
                asQueryGraphEdge(movieCompaniesNode, companyNameNode),
                asQueryGraphEdge(castInfoNode, titleNode),
                asQueryGraphEdge(castInfoNode, charNameNode),
                asQueryGraphEdge(castInfoNode, roleTypeNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                        companyTypeNode.getSchemaTableName(),
                        castInfoNode.getSchemaTableName(),
                        titleNode.getSchemaTableName(),
                        charNameNode.getSchemaTableName(),
                        roleTypeNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
