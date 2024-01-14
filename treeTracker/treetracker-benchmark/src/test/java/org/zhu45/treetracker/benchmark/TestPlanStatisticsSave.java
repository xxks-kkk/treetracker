package org.zhu45.treetracker.benchmark;

import org.openjdk.jmh.runner.RunnerException;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrdering;

import java.util.List;

import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;

/**
 * Manual test the save() of PlanStatistics
 */
public class TestPlanStatisticsSave
{
    public static void main(String[] args)
            throws RunnerException
    {
        JoinFragmentType query = queryProvider(JoinOperator.HASH_JOIN,
                JOBQueries.Q1a,
                List.of(new FindTheBestJoinOrdering()),
                postgresJdbcClientSupplier.get());
        query.getPlan().getPlanStatistics().save(query.getQueryName(),
                query.getAlgorithm(), JOB_WITH_PREDICATES_RESULT_STORED_PATH);
    }
}
