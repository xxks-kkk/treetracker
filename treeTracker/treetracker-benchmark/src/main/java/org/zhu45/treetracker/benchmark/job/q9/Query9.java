package org.zhu45.treetracker.benchmark.job.q9;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query9
        extends Query
{
    public Query9(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q9, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q9);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q9, null);
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q9);
        MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q9);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q9);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q9, null);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(JOBQueries.Q9);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, titleNode),
                asEdge(movieCompaniesNode, castInfoNode),
                asEdge(castInfoNode, charNameNode),
                asEdge(castInfoNode, akaNameNode),
                asEdge(akaNameNode, nameNode),
                asEdge(castInfoNode, roleTypeNode),
                asEdge(movieCompaniesNode, companyNameNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                        titleNode.getSchemaTableName(),
                        castInfoNode.getSchemaTableName(),
                        charNameNode.getSchemaTableName(),
                        akaNameNode.getSchemaTableName(),
                        nameNode.getSchemaTableName(),
                        roleTypeNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
