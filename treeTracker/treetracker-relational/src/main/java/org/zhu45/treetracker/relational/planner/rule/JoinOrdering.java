package org.zhu45.treetracker.relational.planner.rule;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class JoinOrdering
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(JoinOrdering.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private final List<SchemaTableName> schemaTableNameList;

    public JoinOrdering(List<SchemaTableName> schemaTableNameList)
    {
        this.schemaTableNameList = schemaTableNameList;
    }

    public static JoinOrdering getJoinOrderingFromNodes(List<MultiwayJoinNode> nodes)
    {
        return new JoinOrdering(nodes.stream().map(MultiwayJoinNode::getSchemaTableName).collect(Collectors.toList()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JoinOrdering that = (JoinOrdering) o;

        return schemaTableNameList.equals(that.schemaTableNameList);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableNameList);
    }

    public List<SchemaTableName> getSchemaTableNameList()
    {
        return schemaTableNameList;
    }

    @Override
    public String toString()
    {
        return schemaTableNameList.toString();
    }

    /**
     * @return true if the given schemaTableNames permits at least one cross-product free ordering; false otherwise.
     */
    public static boolean checkIfCrossProductsFreeOrderingExists(Collection<SchemaTableName> schemaTableNames,
                                                                 HashMap<SchemaTableName, List<String>> schemaTableName2Attributes)
    {
        if (schemaTableNames.size() == 1) {
            return true;
        }
        // key: attribute, vals: a list of relations that contains the given attribute
        HashMap<String, List<SchemaTableName>> attributeToRelations = new HashMap<>();
        for (SchemaTableName schemaTableName : schemaTableNames) {
            List<String> attributes = requireNonNull(schemaTableName2Attributes.get(schemaTableName));
            for (String attribute : attributes) {
                attributeToRelations.computeIfAbsent(attribute, k -> new ArrayList<>());
                attributeToRelations.get(attribute).add(schemaTableName);
            }
        }
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("attributeToRelations: " + attributeToRelations);
        }
        Queue<List<SchemaTableName>> queue = new LinkedList<>();
        for (List<SchemaTableName> val : attributeToRelations.values()) {
            if (val.size() > 1) {
                queue.add(val);
            }
        }
        if (queue.isEmpty()) {
            return false;
        }
        Set<SchemaTableName> connectedSchemaTableNameList = new HashSet<>(requireNonNull(queue.poll()));
        while (!queue.isEmpty()) {
            int queueSize = queue.size();
            int currentConnectedSchemaTableNameListSize = connectedSchemaTableNameList.size();
            for (int i = 0; i < queueSize; i++) {
                List<SchemaTableName> wrapper = requireNonNull(queue.poll());
                if (connectedSchemaTableNameList.stream().anyMatch(wrapper::contains)) {
                    connectedSchemaTableNameList.addAll(wrapper);
                }
                else {
                    queue.add(wrapper);
                }
            }
            if (connectedSchemaTableNameList.size() == currentConnectedSchemaTableNameListSize) {
                return false;
            }
        }
        return connectedSchemaTableNameList.size() == schemaTableNames.size();
    }
}
