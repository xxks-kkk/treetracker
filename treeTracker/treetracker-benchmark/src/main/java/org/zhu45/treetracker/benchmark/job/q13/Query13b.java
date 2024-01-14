package org.zhu45.treetracker.benchmark.job.q13;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query13b
        extends Query
{
    public Query13b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q13b, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q13b, null);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q13b, null);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q13b, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q13b);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q13b);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q13b, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q13b, TableInstanceId.TWO);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q13b, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, titleNode),
                asEdge(titleNode, kindTypeNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, movieInfoNode),
                asEdge(movieInfoNode, infoTypeNode1),
                asEdge(infoTypeNode1, infoTypeNode2),
                asEdge(movieInfoNode, movieInfoIdxNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                kindTypeNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                companyTypeNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName(),
                movieInfoIdxNode.getSchemaTableName()));
        return pair;
    }
}
