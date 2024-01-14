package org.zhu45.treetracker.benchmark.job.q33;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getLinkTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieLinkInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query33b
        extends Query
{
    public Query33b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode companyNameNode1 = getCompanyNameInt(JOBQueries.Q33b, TableInstanceId.ONE);
        MultiwayJoinNode companyNameNode2 = getCompanyNameInt(JOBQueries.Q33b, TableInstanceId.TWO);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q33b, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q33b, TableInstanceId.TWO);
        MultiwayJoinNode kindTypeNode1 = getKindTypeInt(JOBQueries.Q33b, TableInstanceId.ONE);
        MultiwayJoinNode kindTypeNode2 = getKindTypeInt(JOBQueries.Q33b, TableInstanceId.TWO);
        MultiwayJoinNode linkTypeNode = getLinkTypeInt(JOBQueries.Q33b);
        MultiwayJoinNode movieCompaniesNode1 = getMovieCompaniesInt(JOBQueries.Q33b, TableInstanceId.ONE);
        MultiwayJoinNode movieCompaniesNode2 = getMovieCompaniesInt(JOBQueries.Q33b, TableInstanceId.TWO);
        MultiwayJoinNode movieInfoIdxNode1 = getMovieInfoIdxInt(JOBQueries.Q33b, TableInstanceId.ONE);
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q33b, TableInstanceId.TWO);
        MultiwayJoinNode movieLinkNode = getMovieLinkInt();
        MultiwayJoinNode titleNode1 = getTitleInt(JOBQueries.Q33b, TableInstanceId.ONE);
        MultiwayJoinNode titleNode2 = getTitleInt(JOBQueries.Q33b, TableInstanceId.TWO);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieLinkNode, titleNode2),
                asEdge(titleNode2, movieInfoIdxNode2),
                asEdge(movieInfoIdxNode2, infoTypeNode2),
                asEdge(movieInfoIdxNode2, movieCompaniesNode2),
                asEdge(movieCompaniesNode2, companyNameNode2),
                asEdge(titleNode2, kindTypeNode2),
                asEdge(movieLinkNode, titleNode1),
                asEdge(titleNode1, movieInfoIdxNode1),
                asEdge(movieInfoIdxNode1, infoTypeNode1),
                asEdge(movieInfoIdxNode1, movieCompaniesNode1),
                asEdge(movieCompaniesNode1, companyNameNode1),
                asEdge(titleNode1, kindTypeNode1),
                asEdge(movieLinkNode, linkTypeNode)), movieLinkNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan,
                Arrays.asList(movieLinkNode.getSchemaTableName(),
                        titleNode2.getSchemaTableName(),
                        movieInfoIdxNode2.getSchemaTableName(),
                        infoTypeNode2.getSchemaTableName(),
                        movieCompaniesNode2.getSchemaTableName(),
                        companyNameNode2.getSchemaTableName(),
                        kindTypeNode2.getSchemaTableName(),
                        titleNode1.getSchemaTableName(),
                        movieInfoIdxNode1.getSchemaTableName(),
                        infoTypeNode1.getSchemaTableName(),
                        movieCompaniesNode1.getSchemaTableName(),
                        companyNameNode1.getSchemaTableName(),
                        kindTypeNode1.getSchemaTableName(),
                        linkTypeNode.getSchemaTableName()));
        return pair;
    }
}
