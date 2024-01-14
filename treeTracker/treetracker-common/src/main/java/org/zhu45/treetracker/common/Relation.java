package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.common.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Relation
{
    private final List<Column> columns;
    private final String relationName;
    private final List<Row> rows;

    public Relation(String relationName, List<Column> columns)
    {
        this.relationName = relationName;
        this.columns = requireNonNull(columns, "columns cannot be null");
        this.rows = intoRowOriented();
    }

    public String getRelationName()
    {
        return relationName;
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    public List<Row> getRows()
    {
        return rows;
    }

    /**
     * We only use this constructor for the tmpTable (e.g., join result).
     *
     * @param resultSet a RowSet object
     */
    public Relation(RowSet resultSet)
    {
        this.relationName = resultSet.getName();
        this.rows = requireNonNull(resultSet, "resultSet is null").getRows();
        this.columns = intoColumnOriented(resultSet.getRows());
    }

    public Relation(RowSet resultSet, String relationName)
    {
        this.relationName = relationName;
        this.rows = requireNonNull(resultSet, "resultSet is null").getRows();
        this.columns = intoColumnOriented(resultSet.getRows());
    }

    /**
     * Convert column-oriented table representation into row-oriented table representation
     *
     * @return a bag of rows
     */
    public List<Row> intoRowOriented()
    {
        List<Row> tmp = new ArrayList<>();
        List<Iterator<RelationalValue>> iteratorList = new ArrayList<>();
        List<String> attributes = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        List<Boolean> isProcessed = new ArrayList<>(Collections.nCopies(this.columns.size(), false));
        for (Column column : this.columns) {
            iteratorList.add(column.iterator());
            attributes.add(column.getName());
            types.add(column.getType());
        }
        while (isProcessed.contains(false)) {
            List<RelationalValue> rowTmp = new ArrayList<>();
            for (int i = 0; i < iteratorList.size(); i++) {
                Iterator<RelationalValue> iterator = iteratorList.get(i);
                if (iterator.hasNext()) {
                    rowTmp.add(iterator.next());
                }
                else {
                    rowTmp.add(null);
                    isProcessed.set(i, true);
                }
            }
            if (rowTmp.stream().anyMatch(Objects::nonNull)) {
                tmp.add(new ObjectRow(attributes, rowTmp));
            }
        }
        return tmp;
    }

    private List<Column> intoColumnOriented(List<Row> rows)
    {
        if (rows.isEmpty()) {
            return new ArrayList<>();
        }
        List<Column> columnList = new ArrayList<>();
        int rowLength = rows.get(0).size();
        List<String> attributes = rows.get(0).getAttributes();
        List<Type> types = rows.get(0).getTypes();
        for (int i = 0; i < rowLength; ++i) {
            String columnName = attributes.get(i);
            Column tmp = new Column(columnName, types.get(i), new ArrayList<>());
            for (Row row : rows) {
                tmp.add(row.getVals().get(i));
            }
            columnList.add(tmp);
        }
        return columnList;
    }
}
