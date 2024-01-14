package org.zhu45.treetracker.benchmark.job.q10;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query10a
        extends Query
{
    public Query10a(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q10a, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q10a);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q10a, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q10a);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q10a, null);
        MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q10a);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(JOBQueries.Q10a);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, castInfoNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(castInfoNode, titleNode),
                asEdge(castInfoNode, charNameNode),
                asEdge(castInfoNode, roleTypeNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                companyTypeNode.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                charNameNode.getSchemaTableName(),
                roleTypeNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName()));
        return pair;
    }
}
