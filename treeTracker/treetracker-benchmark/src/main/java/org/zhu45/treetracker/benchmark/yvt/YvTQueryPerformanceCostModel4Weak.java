package org.zhu45.treetracker.benchmark.yvt;

import org.zhu45.treetracker.benchmark.CostModelFactory;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.statistics.CostModel;
import org.zhu45.treetracker.relational.statistics.CostModel4Weak;

import static org.zhu45.treetracker.benchmark.Benchmarks.COST_MODEL_4_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createCostStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;

public class YvTQueryPerformanceCostModel4Weak
{
    private YvTQueryPerformanceCostModel4Weak() {}

    public static void main(String[] args)
    {
        JOBQueries jobQueries = JOBQueries.Q32b;
        CostModelFactory factory = new CostModelFactory(jobQueries, CostModel4Weak.class, postgresJdbcClientSupplier.get());
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, jobQueries, duckDBJdbcClientSupplier.get());
        CostModel costModel = factory.get();
        createCostStatisticsJson(query, costModel, COST_MODEL_4_WITH_PREDICATES_RESULT_STORED_PATH);
    }
}
