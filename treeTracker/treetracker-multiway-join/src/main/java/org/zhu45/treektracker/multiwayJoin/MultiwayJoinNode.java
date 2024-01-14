package org.zhu45.treektracker.multiwayJoin;

import com.google.common.collect.ImmutableList;
import de.renebergelt.test.Switches;
import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcColumnHandle;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * We define a query graph node with each node associated with a relation
 * An example can be seen:
 * <p>
 * Miranker, Daniel P., Roberto J. Bayardo, and Vasilis Samoladas.
 * "Query evaluation as constraint search; an overview of early results."
 * International Workshop on Constraint Database Systems. Springer, Berlin, Heidelberg, 1997.
 */
public class MultiwayJoinNode
        extends BaseNode<MultiwayJoinNode, Row, MultiwayJoinDomain>
{
    private final SchemaTableName schemaTableName;
    private ImmutableList<String> attributes;

    public MultiwayJoinNode(SchemaTableName schemaTableName, MultiwayJoinDomain domain)
    {
        super(schemaTableName.toString(), domain);
        this.schemaTableName = schemaTableName;
    }

    public MultiwayJoinNode(SchemaTableName schemaTableName, List<String> attributes, MultiwayJoinDomain domain)
    {
        super(schemaTableName.toString(), domain);
        this.schemaTableName = schemaTableName;
        this.attributes = ImmutableList.copyOf(attributes);
    }

    public MultiwayJoinNode(MultiwayJoinNode node)
    {
        super(node);
        schemaTableName = new SchemaTableName(node.getSchemaTableName().getSchemaName(), node.getSchemaTableName().getTableName());
        attributes = ImmutableList.copyOf(node.getAttributes());
    }

    /**
     * Populate Domain with rows
     */
    public void populateDomain()
    {
        int i = 0;
        for (Row value : domain) {
            domain.add(value);
            if (Switches.DEBUG) {
                i++;
                if (i % 100 == 0) {
                    System.out.println("number of tuples read so far: " + i);
                }
            }
        }
    }

    public SchemaTableName getSchemaTableName()
    {
        return this.schemaTableName;
    }

    public List<String> getAttributes()
    {
        return this.attributes;
    }

    @Override
    public MultiwayJoinDomain getDomain()
    {
        return (MultiwayJoinDomain) super.getDomain();
    }

    public static MultiwayJoinNode getTableNode(SchemaTableName schemaTableName, JdbcClient jdbcClient)
    {
        JdbcTableHandle tableHandle = jdbcClient.getTableHandle(schemaTableName);
        requireNonNull(tableHandle,
                String.format("Need to setup %s in %s database first", schemaTableName.getTableName(), schemaTableName.getSchemaName()));
        List<JdbcColumnHandle> columnHandles = jdbcClient.getColumns(tableHandle);
        List<String> attributes = columnHandles.stream().map(JdbcColumnHandle::getColumnName).collect(Collectors.toList());
        MultiwayJoinDomain domainTable = new MultiwayJoinDomain();
        return new MultiwayJoinNode(schemaTableName, attributes, domainTable);
    }
}
