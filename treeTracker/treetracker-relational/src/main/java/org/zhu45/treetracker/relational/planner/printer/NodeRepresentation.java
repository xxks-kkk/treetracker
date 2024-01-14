package org.zhu45.treetracker.relational.planner.printer;

import org.zhu45.treetracker.relational.planner.PlanNodeId;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NodeRepresentation
{
    private final PlanNodeId id;
    private final String name;
    private final String identifier;
    private final List<PlanNodeId> children;

    private final StringBuilder details = new StringBuilder();

    public NodeRepresentation(
            PlanNodeId id,
            String name,
            String identifier,
            List<PlanNodeId> children)
    {
        this.id = requireNonNull(id, "id is null");
        this.name = requireNonNull(name, "name is null");
        this.identifier = requireNonNull(identifier, "identifier is null");
        this.children = requireNonNull(children, "children is null");
    }

    public void appendDetails(String string, Object... args)
    {
        if (args.length == 0) {
            details.append(string);
        }
        else {
            details.append(format(string, args));
        }
    }

    public void appendDetailsLine(String string, Object... args)
    {
        appendDetails(string, args);
        details.append('\n');
    }

    public PlanNodeId getId()
    {
        return id;
    }

    public String getName()
    {
        return name;
    }

    public String getIdentifier()
    {
        return identifier;
    }

    public List<PlanNodeId> getChildren()
    {
        return children;
    }

    public String getDetails()
    {
        return details.toString();
    }
}
