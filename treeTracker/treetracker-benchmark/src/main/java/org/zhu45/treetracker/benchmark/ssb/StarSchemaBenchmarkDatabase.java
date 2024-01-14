package org.zhu45.treetracker.benchmark.ssb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.Column;
import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.testing.TestingDatabase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.getProperties;

/**
 * Because we primarily focus on the join evaluation, we make a few simplification to the
 * SSB schema to reduce the development effort. We use 'CUSTKEY', 'SUPPKEY', 'PARTKEY', and 'DATEKEY'
 * through relations and replace any attribute shown in join predicate of SSB queries with one of the aforementioned
 * three attributes. For example, 'LO_ORDERDATE' will be replaced as 'DATEKEY' because in Q1.1, we
 * have the join predicate 'lo_orderdate = d_datekey'. Doing so, we effectively transform theta join into natural join.
 * However, this won't break the main purpose of experimental evaluation, which focuses on multi-way join processing.
 * <p>
 * Also note that LO_COMMITDATE is never used in SSB query and thus, above method works.
 */
@Deprecated
public class StarSchemaBenchmarkDatabase
        extends TestingDatabase
{
    private Logger logger = LogManager.getLogger(StarSchemaBenchmarkDatabase.class);

    public static String ssbSchemaName = "ssb";
    public static String customer = "CUSTOMER";
    public static String lineOrder = "LINEORDER";
    public static String part = "PART";
    public static String supplier = "SUPPLIER";
    public static String date = "DATE";

    public static final String custKey = "CUSTKEY";
    public static final String suppKey = "SUPPKEY";
    public static final String dateKey = "DATEKEY";
    public static final String partKey = "PARTKEY";

    // Development schemas
    public static String ssbSchemaNameDev = "ssb_dev";
    public static String customerDev = "CUSTOMER_DEV";
    public static String lineOrderDev = "LINEORDER_DEV";
    public static String partDev = "PART_DEV";
    public static String supplierDev = "SUPPLIER_DEV";
    public static String dateDev = "DATE_DEV";

    private static final String delimiter = "|";

    Properties properties = getProperties("ssb.properties");

    public StarSchemaBenchmarkDatabase()
            throws SQLException
    {
        super();
        setupDatabase();
    }

    public StarSchemaBenchmarkDatabase(String ssbSchemaName,
            String customer,
            String lineOrder,
            String part,
            String supplier,
            String date,
            String propertyFileName)
            throws SQLException
    {
        super();
        StarSchemaBenchmarkDatabase.ssbSchemaName = ssbSchemaName;
        StarSchemaBenchmarkDatabase.customer = customer;
        StarSchemaBenchmarkDatabase.lineOrder = lineOrder;
        StarSchemaBenchmarkDatabase.part = part;
        StarSchemaBenchmarkDatabase.supplier = supplier;
        StarSchemaBenchmarkDatabase.date = date;
        properties = getProperties(propertyFileName);
        setupDatabase();
    }

    private void setupDatabase()
            throws SQLException
    {
        try (Connection connection = getJdbcClient().getConnection()) {
            connection.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", ssbSchemaName));
            connection.createStatement().execute(String.format("CREATE SCHEMA %s", ssbSchemaName));
        }
        logger.info("Start ingestion ...");
        createCustomerTable();
        logger.info("Finish " + customer + " ingestion ...");
        createLineOrder();
        logger.info("Finish " + lineOrder + " ingestion ...");
        createPart();
        logger.info("Finish " + part + " ingestion ...");
        createSupplier();
        logger.info("Finish " + supplier + " ingestion ...");
        createDate();
        logger.info("Finish " + date + " ingestion ...");
    }

    private void createCustomerTable()
    {
        List<Column> columnList = Arrays.asList(
                new Column(custKey, VARCHAR, new ArrayList<>()),
                new Column("C_NAME", VARCHAR, new ArrayList<>()),
                new Column("C_ADDRESS", VARCHAR, new ArrayList<>()),
                new Column("C_CITY", VARCHAR, new ArrayList<>()),
                new Column("C_NATION", VARCHAR, new ArrayList<>()),
                new Column("C_REGION", VARCHAR, new ArrayList<>()),
                new Column("C_PHONE", VARCHAR, new ArrayList<>()),
                new Column("C_MKTSEGMENT", VARCHAR, new ArrayList<>()));
        Relation relation = new Relation(customer, columnList);
        createTableCommon(relation, "customer.data");
    }

    private void createLineOrder()
    {
        List<Column> columnList = Arrays.asList(
                new Column("LO_ORDERKEY", VARCHAR, new ArrayList<>()),
                new Column("LO_LINENUMBER", VARCHAR, new ArrayList<>()),
                new Column(custKey, VARCHAR, new ArrayList<>()),
                new Column(partKey, VARCHAR, new ArrayList<>()),
                new Column(suppKey, VARCHAR, new ArrayList<>()),
                new Column(dateKey, VARCHAR, new ArrayList<>()),
                new Column("LO_ORDERPRIORITY", VARCHAR, new ArrayList<>()),
                new Column("LO_SHIPPRIORITY", VARCHAR, new ArrayList<>()),
                new Column("LO_QUANTITY", VARCHAR, new ArrayList<>()),
                new Column("LO_EXTENDEDPRICE", VARCHAR, new ArrayList<>()),
                new Column("LO_ORDTOTALPRICE", VARCHAR, new ArrayList<>()),
                new Column("LO_DISCOUNT", VARCHAR, new ArrayList<>()),
                new Column("LO_REVENUE", VARCHAR, new ArrayList<>()),
                new Column("LO_SUPPLYCOST", VARCHAR, new ArrayList<>()),
                new Column("LO_TAX", VARCHAR, new ArrayList<>()),
                new Column("LO_COMMITDATE", VARCHAR, new ArrayList<>()),
                new Column("LO_SHIPMODE", VARCHAR, new ArrayList<>()));
        Relation relation = new Relation(lineOrder, columnList);
        createTableCommon(relation, "lineorder.data");
    }

    private void createPart()
    {
        List<Column> columnList = Arrays.asList(
                new Column(partKey, VARCHAR, new ArrayList<>()),
                new Column("P_NAME", VARCHAR, new ArrayList<>()),
                new Column("P_MFGR", VARCHAR, new ArrayList<>()),
                new Column("P_CATEGORY", VARCHAR, new ArrayList<>()),
                new Column("P_BRAND1", VARCHAR, new ArrayList<>()),
                new Column("P_COLOR", VARCHAR, new ArrayList<>()),
                new Column("P_TYPE", VARCHAR, new ArrayList<>()),
                new Column("P_SIZE", VARCHAR, new ArrayList<>()),
                new Column("P_CONTAINER", VARCHAR, new ArrayList<>()));
        Relation relation = new Relation(part, columnList);
        createTableCommon(relation, "part.data");
    }

    private void createSupplier()
    {
        List<Column> columnList = Arrays.asList(
                new Column(suppKey, VARCHAR, new ArrayList<>()),
                new Column("S_NAME", VARCHAR, new ArrayList<>()),
                new Column("S_ADDRESS", VARCHAR, new ArrayList<>()),
                new Column("S_CITY", VARCHAR, new ArrayList<>()),
                new Column("S_NATION", VARCHAR, new ArrayList<>()),
                new Column("S_REGION", VARCHAR, new ArrayList<>()),
                new Column("S_PHONE", VARCHAR, new ArrayList<>()));
        Relation relation = new Relation(supplier, columnList);
        createTableCommon(relation, "supplier.data");
    }

    private void createDate()
    {
        List<Column> columnList = Arrays.asList(
                new Column(dateKey, VARCHAR, new ArrayList<>()),
                new Column("D_DATE", VARCHAR, new ArrayList<>()),
                new Column("D_DAYOFWEEK", VARCHAR, new ArrayList<>()),
                new Column("D_MONTH", VARCHAR, new ArrayList<>()),
                new Column("D_YEAR", VARCHAR, new ArrayList<>()),
                new Column("D_YEARMONTHNUM", VARCHAR, new ArrayList<>()),
                new Column("D_YEARMONTH", VARCHAR, new ArrayList<>()),
                new Column("D_DAYNUMINWEEK", VARCHAR, new ArrayList<>()),
                new Column("D_DAYNUMINMONTH", VARCHAR, new ArrayList<>()),
                new Column("D_DAYNUMINYEAR", VARCHAR, new ArrayList<>()),
                new Column("D_MONTHNUMINYEAR", VARCHAR, new ArrayList<>()),
                new Column("D_WEEKNUMINYEAR", VARCHAR, new ArrayList<>()),
                new Column("D_SELLINGSEASON", VARCHAR, new ArrayList<>()),
                new Column("D_LASTDAYINWEEKFL", VARCHAR, new ArrayList<>()),
                new Column("D_LASTDAYINMONTHFL", VARCHAR, new ArrayList<>()),
                new Column("D_HOLIDAYFL", VARCHAR, new ArrayList<>()),
                new Column("D_WEEKDAYFL", VARCHAR, new ArrayList<>()));
        Relation relation = new Relation(date, columnList);
        createTableCommon(relation, "date.data");
    }

    private void createTableCommon(Relation relation, String propertyKey)
    {
        JdbcTableHandle tableHandle = getJdbcClient().getTableHandle(new SchemaTableName(ssbSchemaName, relation.getRelationName()));
        if (tableHandle != null) {
            getJdbcClient().deleteTable(tableHandle);
        }
        else {
            getJdbcClient().createTable(ssbSchemaName, relation);
        }
        getJdbcClient().loadTable(new SchemaTableName(ssbSchemaName, relation.getRelationName()), properties.getProperty(propertyKey), delimiter);
    }
}
