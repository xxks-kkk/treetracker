package org.zhu45.treetracker.benchmark.job.q19;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query19b
        extends Query
{
    public Query19b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q19b);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(JOBQueries.Q19b);
        MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q19b);
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q19b);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q19b);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q19b, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q19b, null);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q19b);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q19b, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q19b, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(castInfoNode, roleTypeNode),
                asEdge(castInfoNode, charNameNode),
                asEdge(castInfoNode, akaNameNode),
                asEdge(akaNameNode, nameNode),
                asEdge(castInfoNode, movieCompaniesNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, movieInfoNode),
                asEdge(movieInfoNode, titleNode),
                asEdge(movieInfoNode, infoTypeNode)), castInfoNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(castInfoNode.getSchemaTableName(),
                roleTypeNode.getSchemaTableName(),
                charNameNode.getSchemaTableName(),
                akaNameNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                movieCompaniesNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                infoTypeNode.getSchemaTableName()));
        return pair;
    }
}
