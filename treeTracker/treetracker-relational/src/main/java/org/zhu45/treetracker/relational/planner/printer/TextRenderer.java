package org.zhu45.treetracker.relational.planner.printer;

import com.google.common.base.Strings;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class TextRenderer
{
    private final int level;

    public TextRenderer(int level)
    {
        this.level = level;
    }

    public String render(PlanRepresentation plan)
    {
        StringBuilder output = new StringBuilder();
        return writeTextOutput(output, plan, plan.getRoot(), level);
    }

    private String writeTextOutput(StringBuilder output,
            PlanRepresentation plan,
            NodeRepresentation node,
            int level)
    {
        output.append(indentString(level))
                .append("- ")
                .append(String.format("[%s]", node.getId()))
                .append(node.getName())
                .append(" ")
                .append(node.getIdentifier())
                .append("\n");

        if (!node.getDetails().isEmpty()) {
            String details = indentMultilineString(node.getDetails(), level + 2);
            output.append(details);
            if (!details.endsWith("\n")) {
                output.append('\n');
            }
        }

        List<NodeRepresentation> children = node.getChildren().stream()
                .map(plan::getNode)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        for (NodeRepresentation child : children) {
            writeTextOutput(output, plan, child, level + 1);
        }

        return output.toString();
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private static String indentMultilineString(String string, int level)
    {
        return string.replaceAll("(?m)^", indentString(level));
    }
}
