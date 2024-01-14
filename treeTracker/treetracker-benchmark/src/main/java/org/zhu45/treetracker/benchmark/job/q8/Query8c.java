package org.zhu45.treetracker.benchmark.job.q8;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query8c
        extends Query
{
    public Query8c(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q8c);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q8c);
        MultiwayJoinNode castInfNode = getCastInfoInt(JOBQueries.Q8c);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q8c, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q8c, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q8c, null);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(JOBQueries.Q8c);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(akaNameNode, nameNode),
                asEdge(nameNode, castInfNode),
                asEdge(castInfNode, movieCompaniesNode),
                asEdge(castInfNode, roleTypeNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, titleNode)), castInfNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(castInfNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                akaNameNode.getSchemaTableName(),
                movieCompaniesNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                roleTypeNode.getSchemaTableName()));
        return pair;
    }
}
