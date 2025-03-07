package org.zhu45.treetracker.relational.planner.catalog;

import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TableHandle;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcColumnHandle;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Catalog for a table
 */
public class TableCatalog
{
    private final JdbcClient jdbcClient;
    private final SchemaTableName schemaTableName;
    private final TableHandle tableHandle;
    private final List<JdbcColumnHandle> columnHandles;

    // Column types associated with the table.
    private final List<Type> typeList;
    // Column names associated with the table.
    private final List<String> attributes;
    // The fraction of unique value in each column
    private final float[] fractionOfUniquesInEachColumn;
    // the number of columns that have all unique values
    private int numColumnsHaveAllUniqueValues;

    private TableCatalog(SchemaTableName schemaTableName, JdbcClient jdbcClient)
    {
        this.schemaTableName = schemaTableName;
        this.jdbcClient = jdbcClient;
        this.tableHandle = requireNonNull(jdbcClient.getTableHandle(schemaTableName), schemaTableName + " not exists");
        this.columnHandles = requireNonNull(jdbcClient.getColumns((JdbcTableHandle) tableHandle));
        this.typeList = initializeTypeList();
        this.attributes = initializeAttributes();
        this.fractionOfUniquesInEachColumn = initializeFractionOfUniquesInEachColumn();
    }

    public TableCatalog(SchemaTableName schemaTableName, List<Type> typeList, List<String> attributes)
    {
        this.schemaTableName = schemaTableName;
        this.jdbcClient = null;
        this.tableHandle = null;
        this.columnHandles = null;
        this.typeList = typeList;
        this.attributes = attributes;
        this.fractionOfUniquesInEachColumn = null;
    }

    public SchemaTableName getTableName()
    {
        return schemaTableName;
    }

    public static TableCatalog initializeTableCatalog(SchemaTableName schemaTableName, JdbcClient jdbcClient)
    {
        return new TableCatalog(schemaTableName, jdbcClient);
    }

    private List<Type> initializeTypeList()
    {
        List<Type> typeList = new ArrayList<>();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            typeList.add(columnHandle.getColumnType());
        }
        return typeList;
    }

    private List<String> initializeAttributes()
    {
        List<String> attributes = new ArrayList<>();
        for (JdbcColumnHandle columnHandle : columnHandles) {
            attributes.add(columnHandle.getColumnName());
        }
        return attributes;
    }

    private float[] initializeFractionOfUniquesInEachColumn()
    {
        float[] fractionOfUniquesInEachColumn = new float[typeList.size()];
        for (int i = 0; i < fractionOfUniquesInEachColumn.length; ++i) {
            // we probably instead maintain the degree of uniqueness (# of unique values / total values), i.e., use float[] instead of boolean[].
            // if 100%, then the column is all unique. If every R_k join idx (with its child) is 100%, then we don't need to use noGoodListMap at
            // all and if not, we choose the one with the lowest percentage to issue order by (not in this commit by delivering separately via
            // #247). Thus, we probably want to repurpose jdbcClient API
            fractionOfUniquesInEachColumn[i] = jdbcClient.fractionOfUniqueValuesInColumn((JdbcTableHandle) tableHandle, List.of(columnHandles.get(i)));
            if (Float.compare(fractionOfUniquesInEachColumn[i], 1.0f) == 0) {
                numColumnsHaveAllUniqueValues++;
            }
        }
        return fractionOfUniquesInEachColumn;
    }

    public TableHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<JdbcColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }

    public List<Type> getTypeList()
    {
        return typeList;
    }

    public float[] getFractionOfUniquesInEachColumn()
    {
        return fractionOfUniquesInEachColumn;
    }

    public int getNumColumnsHaveAllUniqueValues()
    {
        return numColumnsHaveAllUniqueValues;
    }

    public List<String> getAttributes()
    {
        return attributes;
    }

    public long getSize()
    {
        //NOTE: the reason we don't store this information inside memory is that
        //in the long term, we want to store those information outside in some catalog service,
        //e.g., special tables in Postgres.
        try (Connection connection = jdbcClient.getConnection()) {
            return jdbcClient.getTableSize(connection, (JdbcTableHandle) tableHandle);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
