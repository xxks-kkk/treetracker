package org.zhu45.treetracker.relational.planner.plan;

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
    private final SchemaTableName schemaTableName;
    private static final OptType NODE_PLAN_NODE_TYPE = OptType.table;
    private JdbcClient jdbcClient;
    private final MultiwayJoinNode multiwayJoinNode;

    public TableNode(PlanNodeId id, SchemaTableName schemaTableName)
    {
        super(id);

        this.jdbcClient = null;
        this.schemaTableName = schemaTableName;
        this.multiwayJoinNode = null;
    }

    public TableNode(PlanNodeId id, MultiwayJoinNode multiwayJoinNode)
    {
        super(id);
        requireNonNull(multiwayJoinNode, "multiwayJoinNode is null");

        this.jdbcClient = null;
        this.schemaTableName = multiwayJoinNode.getSchemaTableName();
        this.multiwayJoinNode = multiwayJoinNode;
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

    public SchemaTableName getSchemaTableName()
    {
        return this.schemaTableName;
    }

    public MultiwayJoinNode getMultiwayJoinNode()
    {
        return this.multiwayJoinNode;
    }
}
