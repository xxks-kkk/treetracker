package org.zhu45.treetracker.benchmark.job.q17;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query1711
        extends Query
{
    public Query1711(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q17, null);
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q17, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q17);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q17);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q17, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q17);

        // Inspired from Q311
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, castInfoNode),
                asEdge(castInfoNode, nameNode),
                asEdge(movieCompaniesNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(movieCompaniesNode, companyNameNode),
                asEdge(movieCompaniesNode, titleNode)), movieCompaniesNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(movieCompaniesNode.getSchemaTableName(),
                        castInfoNode.getSchemaTableName(),
                        nameNode.getSchemaTableName(),
                        movieKeywordNode.getSchemaTableName(),
                        keywordNode.getSchemaTableName(),
                        companyNameNode.getSchemaTableName(),
                        titleNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
