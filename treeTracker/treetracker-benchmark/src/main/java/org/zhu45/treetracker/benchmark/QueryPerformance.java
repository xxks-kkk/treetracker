package org.zhu45.treetracker.benchmark;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.ssb.SSBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class QueryPerformance
{
    private QueryPerformance()
    {
    }

    public static void main(String[] args)
    {
        SSBQueries queries = SSBQueries.Q3P4;
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queries, duckDBJdbcClientSupplier.get());
        try {
            query.eval();
        }
        finally {
            query.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
        }
    }
}
