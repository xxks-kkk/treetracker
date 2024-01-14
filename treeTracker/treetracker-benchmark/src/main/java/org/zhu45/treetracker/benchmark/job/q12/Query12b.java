package org.zhu45.treetracker.benchmark.job.q12;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query12b
        extends Query
{
    public Query12b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q12b, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q12b, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q12b, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q12b);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q12b);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q12b, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q12b, TableInstanceId.TWO);
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q12b, TableInstanceId.TWO);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, titleNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, movieInfoNode),
                asEdge(movieInfoNode, infoTypeNode1),
                asEdge(infoTypeNode1, infoTypeNode2),
                asEdge(movieInfoNode, movieInfoIdxNode2)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                companyTypeNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName(),
                movieInfoIdxNode2.getSchemaTableName()));
        return pair;
    }
}
