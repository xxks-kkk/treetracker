package org.zhu45.treetracker.relational.planner.plan;

import lombok.Getter;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeId;
import org.zhu45.treetracker.relational.planner.PlanVisitor;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class TableNode
        extends PlanNode
{
    private static final OptType NODE_PLAN_NODE_TYPE = OptType.table;
    private JdbcClient jdbcClient;
    @Getter
    private MultiwayJoinNode multiwayJoinNode;

    public TableNode(PlanNodeId id, SchemaTableName schemaTableName, Side side)
    {
        super(id, side);

        this.schemaTableName = schemaTableName;
    }

    public TableNode(PlanNodeId id, MultiwayJoinNode multiwayJoinNode, Side side)
    {
        super(id, side);
        this.multiwayJoinNode = requireNonNull(multiwayJoinNode, "multiwayJoinNode is null");
        this.schemaTableName = multiwayJoinNode.getSchemaTableName();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.emptyList();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTable(this, context);
    }

    @Override
    public OptType getNodeType()
    {
        return NODE_PLAN_NODE_TYPE;
    }

    public void setJdbcClient(JdbcClient jdbcClient)
    {
        requireNonNull(jdbcClient, "jdbcClient cannot be null");
        this.jdbcClient = jdbcClient;
    }

    @Override
    public String toString()
    {
        return schemaTableName.toString();
    }
}
