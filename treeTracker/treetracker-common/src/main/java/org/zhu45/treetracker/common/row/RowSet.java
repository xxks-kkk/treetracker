package org.zhu45.treetracker.common.row;

import com.google.common.base.Strings;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.Utils;
import org.zhu45.treetracker.common.Value;
import org.zhu45.treetracker.common.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RowSet<R extends Row>
        implements Value
{
    private final List<R> rows;
    private static final String tmpName = "tmpTable";

    public RowSet(Iterable<R> rows)
    {
        this.rows = FastList.newList();
        rows.forEach(this.rows::add);
    }

    public RowSet()
    {
        this.rows = FastList.newList();
    }

    public List<R> getRows()
    {
        return this.rows;
    }

    public String getName()
    {
        return tmpName;
    }

    public void add(R row)
    {
        this.rows.add(row);
    }

    public void remove(Row row)
    {
        this.rows.remove(row);
    }

    public void addAll(List<R> rows)
    {
        this.rows.addAll(rows);
    }

    public void addAll(RowSet<R> rowSet)
    {
        this.rows.addAll(rowSet.getRows());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        RowSet<R> o = (RowSet<R>) obj;
        // We ignore the ordering as the ordering of rows doesn't matter.
        return Utils.collectionCompare(this.rows, o.rows, ObjectRow::equals);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rows);
    }

    public int size()
    {
        return this.rows.size();
    }

    /**
     * Used by RowSet renderOutput() to render the RowSet into a tabular format
     *
     * @return String
     */
    public String renderOutput()
    {
        int maxColumnWidth = 20;

        StringBuilder table = new StringBuilder();
        if (!rows.isEmpty()) {
            List<String> attributes = rows.get(0).getAttributes();
            List<Type> types = rows.get(0).getTypes();
            List<Integer> printBarPosition = new ArrayList<>();
            int separateLineLength = 1;
            for (int i = 0; i < attributes.size(); ++i) {
                int typeLength = Math.min(types.get(i).getLength(), maxColumnWidth);
                int attributeLength = attributes.get(i).length();
                table.append(attributes.get(i))
                        .append(Strings.repeat(" ", typeLength - attributeLength))
                        .append("|");
                printBarPosition.add(typeLength + 1);
                separateLineLength += typeLength;
            }
            table.append("\n")
                    .append(Strings.repeat("-", separateLineLength))
                    .append("+")
                    .append("\n");
            for (Row row : rows) {
                for (int i = 0; i < row.getVals().size(); ++i) {
                    RelationalValue val = row.getVals().get(i);
                    table.append(val)
                            .append(Strings.repeat(" ", printBarPosition.get(i) - 1 - val.toString().length()))
                            .append("|");
                }
                table.append("\n")
                        .append(Strings.repeat("-", separateLineLength))
                        .append("+")
                        .append("\n");
            }
        }
        return table.toString();
    }
}
