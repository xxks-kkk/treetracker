package org.zhu45.treetracker.benchmark.job.q8;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.benchmark.job.JOBQueries.Q8;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query87
        extends Query
{
    public Query87(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode akaNameNode = getAkaNameInt(Q8);
        MultiwayJoinNode nameNode = getNameInt(Q8);
        MultiwayJoinNode castInfNode = getCastInfoInt(Q8);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(Q8, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(Q8, null);
        MultiwayJoinNode titleNode = getTitleInt(Q8, null);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(Q8);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(castInfNode, nameNode),
                asEdge(castInfNode, movieCompaniesNode),
                asEdge(castInfNode, roleTypeNode),
                asEdge(castInfNode, akaNameNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, titleNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                        castInfNode.getSchemaTableName(),
                        nameNode.getSchemaTableName(),
                        roleTypeNode.getSchemaTableName(),
                        akaNameNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName(),
                        titleNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
