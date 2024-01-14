package org.zhu45.treetracker.benchmark.job.q1;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query1a
        extends Query
{
    public Query1a(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q1a, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q1a);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q1a, null);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q1a, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q1a, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, titleNode),
                asEdge(movieCompaniesNode, movieInfoIdxNode),
                asEdge(movieInfoIdxNode, infoTypeNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);

        verifyJoinOrdering(pair.getKey(), Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                companyTypeNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                movieInfoIdxNode.getSchemaTableName(),
                infoTypeNode.getSchemaTableName()));
        return pair;
    }
}
