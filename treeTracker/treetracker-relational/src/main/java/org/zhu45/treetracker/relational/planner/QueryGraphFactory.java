package org.zhu45.treetracker.relational.planner;

import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.RuntimeContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class QueryGraphFactory
{
    private QueryGraphFactory()
    {
    }

    public static MultiwayJoinGraph createQueryGraph(SqlNode root, RuntimeContext runtimeContext)
    {
        GatherAllRelations gatherAllRelations = new GatherAllRelations(runtimeContext);
        root.accept(gatherAllRelations);
        List<MultiwayJoinNode> nodes = gatherAllRelations.getNodes();
        List<Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeList = new ArrayList<>();
        for (MultiwayJoinNode node1 : nodes) {
            for (MultiwayJoinNode node2 : nodes) {
                if (!node1.equals(node2)) {
                    JoinTreeEdgeWeight edgeWeight = new JoinTreeEdgeWeight(node1, node2);
                    if (edgeWeight.getWeight() != 0) {
                        Edge<MultiwayJoinNode, Row, MultiwayJoinDomain> edge1 = asEdge(node1, node2, edgeWeight);
                        Edge<MultiwayJoinNode, Row, MultiwayJoinDomain> edge2 = asEdge(node2, node1, edgeWeight);
                        if (!edgeList.contains(edge1) && !edgeList.contains(edge2)) {
                            edgeList.add(edge1);
                        }
                    }
                }
            }
        }
        return new MultiwayJoinGraph(edgeList);
    }

    public static class GatherAllRelations
            extends SqlBasicVisitor<Void>
    {
        RuntimeContext context;
        List<MultiwayJoinNode> nodes;

        public GatherAllRelations(RuntimeContext context)
        {
            this.context = context;
            nodes = new ArrayList<>();
        }

        public List<MultiwayJoinNode> getNodes()
        {
            return nodes;
        }

        @Override
        public Void visit(SqlCall node)
        {
            if (node instanceof SqlSelect) {
                visitSqlSelect((SqlSelect) node);
            }
            else if (node instanceof SqlJoin) {
                visitSqlJoin((SqlJoin) node);
            }
            else if (node instanceof SqlBasicCall) {
                visitSqlBasicCall((SqlBasicCall) node);
            }
            return null;
        }

        private void visitSqlSelect(SqlSelect node)
        {
            requireNonNull(node.getFrom()).accept(this);
        }

        private void visitSqlJoin(SqlJoin node)
        {
            if (node.getRight() != null) {
                node.getRight().accept(this);
            }
            if (node.getLeft() != null) {
                node.getLeft().accept(this);
            }
        }

        private void visitSqlBasicCall(SqlBasicCall node)
        {
            if (node.getOperator() instanceof SqlAsOperator) {
                String tableName = node.getOperandList().get(0).toString().toLowerCase(Locale.ENGLISH);
                SchemaTableName schemaTableName = new SchemaTableName(context.getSchema(), tableName);
                nodes.add(new MultiwayJoinNode(schemaTableName,
                        context.getJdbcClient().getAttributes(schemaTableName),
                        new MultiwayJoinDomain()));
            }
        }
    }
}
