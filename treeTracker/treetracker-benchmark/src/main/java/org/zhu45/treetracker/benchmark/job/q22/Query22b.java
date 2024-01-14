package org.zhu45.treetracker.benchmark.job.q22;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query22b
        extends Query
{
    public Query22b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q22b, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q22b);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q22b, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q22b);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q22b);
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q22b, TableInstanceId.TWO);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q22b, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q22b, TableInstanceId.TWO);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q22b, null);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q22b, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, movieInfoNode),
                asEdge(movieInfoNode, movieInfoIdxNode2),
                asEdge(movieInfoIdxNode2, infoTypeNode1),
                asEdge(infoTypeNode1, infoTypeNode2),
                asEdge(movieCompaniesNode, titleNode),
                asEdge(titleNode, kindTypeNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                companyTypeNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                movieInfoIdxNode2.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                kindTypeNode.getSchemaTableName()));
        return pair;
    }
}
