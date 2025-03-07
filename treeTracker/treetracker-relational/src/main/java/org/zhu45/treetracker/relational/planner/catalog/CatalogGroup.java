package org.zhu45.treetracker.relational.planner.catalog;

import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.relational.planner.catalog.TableCatalog.initializeTableCatalog;

public class CatalogGroup
{
    private final Map<SchemaTableName, TableCatalog> catalogs = new HashMap<>();

    private CatalogGroup(List<TableCatalog> catalogList)
    {
        catalogList.forEach(tableCatalog -> catalogs.put(tableCatalog.getTableName(), tableCatalog));
    }

    public static CatalogGroup initializeCatalogGroup(List<SchemaTableName> schemaTableNameList, JdbcClient jdbcClient)
    {
        List<TableCatalog> catalogList = new ArrayList<>();
        for (SchemaTableName schemaTableName : schemaTableNameList) {
            catalogList.add(initializeTableCatalog(schemaTableName, jdbcClient));
        }
        return new CatalogGroup(catalogList);
    }

    public TableCatalog getTableCatalog(SchemaTableName schemaTableName)
    {
        return catalogs.get(schemaTableName);
    }

    public void addTableCatalog(SchemaTableName schemaTableName, TableCatalog tableCatalog)
    {
        checkState(catalogs.put(schemaTableName, tableCatalog) == null, schemaTableName + " already exists in CatalogGroup");
    }
}
