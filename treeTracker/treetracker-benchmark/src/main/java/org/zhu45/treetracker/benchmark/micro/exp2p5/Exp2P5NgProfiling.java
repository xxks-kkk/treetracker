package org.zhu45.treetracker.benchmark.micro.exp2p5;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.List;

import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class Exp2P5NgProfiling
{
    private Exp2P5NgProfiling()
    {
    }

    public static void run(JoinOperator algorithm, Class<? extends Query> queryClazz)
    {
        System.out.println("JoinOperator: " + algorithm);
        System.out.println("queryClazz: " + queryClazz);
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(algorithm)
                .setQueryClazz(queryClazz)
                .setRules(List.of())
                .setJdbcClient(duckDBJdbcClientSupplier.get())
                .build();
        JoinFragmentType query = queryProvider(context);
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

    public static void main(String[] args)
    {
        run(JoinOperator.TTJHP, Exp2P5Query0P.class);
    }
}
