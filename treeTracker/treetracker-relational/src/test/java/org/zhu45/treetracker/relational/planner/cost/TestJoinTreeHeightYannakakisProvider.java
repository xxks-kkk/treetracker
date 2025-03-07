package org.zhu45.treetracker.relational.planner.cost;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.rule.FindOptimalJoinTree;
import org.zhu45.treetracker.relational.planner.rule.RuleStatistics;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.cost.TestJoinTreeHeightProvider.createJOB10a;

public class TestJoinTreeHeightYannakakisProvider
{
    private static final String naturalJoinTable = "TestJoinTreeHeightYannakakisProvider";

    /**
     * This test has a strong dependency with the test() in TestJoinTreeHeightProvider
     */
    @Test
    public void test()
            throws Exception
    {
        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();
        TestingPhysicalPlanBase base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new FindOptimalJoinTree(new JoinTreeHeightYannakakisProvider())),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        Pair<Plan, List<Operator>> plan = createJOB10a(base);
        PlanStatistics planStatistics = plan.getKey().getPlanStatistics();
        /**
         * imdb.q10a_cast_info(person_id,movie_id,person_role_id,role_id)
         * |imdb.q10a_role_type(role_id)
         * |imdb.q10a_title(movie_id,kind_id)
         * |imdb_int.movie_companies(movie_id,company_id,company_type_id)
         * ||imdb.q10a_company_name(company_id)
         * ||imdb_int.company_type(company_type_id)
         * |imdb_int.char_name(person_role_id)
         */
        assertEquals(3.0, planStatistics.getCost());
        RuleStatistics ruleStatistics = planStatistics.getRuleStatisticsList().get(0);
        SemiJoinOrdering semiJoinOrdering = ruleStatistics.getSemiJoinOrdering();
        List<Pair<SchemaTableName, SchemaTableName>> bottomUpPass = semiJoinOrdering.getBottomUpPass()
                .stream().map(pair -> Pair.of(pair.getLeft().getSchemaTableName(), pair.getRight().getSchemaTableName()))
                .collect(Collectors.toList());
        // NOTE: the following two variables should have the same values with the ones used in createJOB10a().
        String schemaName = base.getDatabase().getSchemaName();
        String prefix = "tJTHeight_";
        List<Pair<SchemaTableName, SchemaTableName>> expectedBottomUpPass = List.of(
                Pair.of(new SchemaTableName(schemaName, prefix + "movie_companies"),
                        new SchemaTableName(schemaName, prefix + "company_name")),
                Pair.of(new SchemaTableName(schemaName, prefix + "movie_companies"),
                        new SchemaTableName(schemaName, prefix + "company_type")),
                Pair.of(new SchemaTableName(schemaName, prefix + "cast_info"),
                        new SchemaTableName(schemaName, prefix + "role_type")),
                Pair.of(new SchemaTableName(schemaName, prefix + "cast_info"),
                        new SchemaTableName(schemaName, prefix + "title")),
                Pair.of(new SchemaTableName(schemaName, prefix + "cast_info"),
                        new SchemaTableName(schemaName, prefix + "movie_companies")),
                Pair.of(new SchemaTableName(schemaName, prefix + "cast_info"),
                        new SchemaTableName(schemaName, prefix + "char_name")));
        assertEquals(expectedBottomUpPass, bottomUpPass);
        base.tearDown();
    }
}
