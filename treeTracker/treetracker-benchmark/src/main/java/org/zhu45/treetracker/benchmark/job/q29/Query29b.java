package org.zhu45.treetracker.benchmark.job.q29;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompCastTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompleteCastInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getPersonInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getRoleTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query29b
        extends Query
{
    public Query29b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode completeCastNode = getCompleteCastInt();
        MultiwayJoinNode compCastTypeNode1 = getCompCastTypeInt(JOBQueries.Q29b, TableInstanceId.ONE);
        MultiwayJoinNode compCastTypeNode2 = getCompCastTypeInt(JOBQueries.Q29b, TableInstanceId.TWO);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q29b);
        MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q29b);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q29b);
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q29b);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q29b, null);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q29b, null);
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q29b);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q29b);
        MultiwayJoinNode roleTypeNode = getRoleTypeInt(JOBQueries.Q29b);
        MultiwayJoinNode personInfoNode = getPersonInfoInt(JOBQueries.Q29b);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q29b, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q29b, TableInstanceId.TWO);
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q29b, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(completeCastNode, compCastTypeNode1),
                asEdge(completeCastNode, compCastTypeNode2),
                asEdge(completeCastNode, movieCompaniesNode),
                asEdge(movieCompaniesNode, movieInfoNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(completeCastNode, castInfoNode),
                asEdge(castInfoNode, nameNode),
                asEdge(nameNode, akaNameNode),
                asEdge(castInfoNode, roleTypeNode),
                asEdge(castInfoNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(castInfoNode, charNameNode),
                asEdge(castInfoNode, titleNode),
                asEdge(castInfoNode, personInfoNode),
                asEdge(personInfoNode, infoTypeNode1),
                asEdge(infoTypeNode1, infoTypeNode2)), completeCastNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(completeCastNode.getSchemaTableName(),
                compCastTypeNode1.getSchemaTableName(),
                compCastTypeNode2.getSchemaTableName(),
                movieCompaniesNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                companyNameNode.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                akaNameNode.getSchemaTableName(),
                roleTypeNode.getSchemaTableName(),
                movieKeywordNode.getSchemaTableName(),
                keywordNode.getSchemaTableName(),
                charNameNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                personInfoNode.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName()));
        return pair;
    }
}
