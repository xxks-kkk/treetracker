package org.zhu45.treetracker.relational.planner;

import org.apache.calcite.sql.SqlNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcSupplier;
import org.zhu45.treetracker.relational.PostgresSQLRuntimeContext;
import org.zhu45.treetracker.relational.parser.SqlParser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.StandardErrorCode.INVALID_USAGE;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.planner.QueryGraphFactory.createQueryGraph;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestQueryGraphFactory
{
    SqlParser parser;
    PostgresSQLRuntimeContext runtimeContext;
    String testSchema = "imdb";

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(SqlParser.class.getName(), Level.DEBUG);
        }

        parser = new SqlParser();
        runtimeContext = PostgresSQLRuntimeContext.builder()
                .setJdbcClient(JdbcSupplier.postgresJdbcClientSupplier.get())
                .setSchema(testSchema)
                .build();
        setupDatabase();
    }

    private void setupDatabase() {
        runtimeContext.getJdbcClient().createSchema(testSchema);

        String company_type = "company_type";
        SchemaTableName company_type_schema = new SchemaTableName(testSchema, company_type);
        if (runtimeContext.getJdbcClient().getTableHandle(company_type_schema) == null) {
            List<List<RelationalValue>> relational_val = new ArrayList<>(List.of(
                    Arrays.asList(new IntegerValue(1))));
            runtimeContext.getJdbcClient().ingestRelation(
                    testSchema,
                    company_type,
                    new ArrayList<>(Arrays.asList("company_type_id")),
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relational_val);
        }

        String info_type = "info_type";
        SchemaTableName info_type_schema = new SchemaTableName(testSchema, info_type);
        if (runtimeContext.getJdbcClient().getTableHandle(info_type_schema) == null) {
            List<List<RelationalValue>> relational_val = new ArrayList<>(List.of(
                    Arrays.asList(new IntegerValue(1))));
            runtimeContext.getJdbcClient().ingestRelation(
                    testSchema,
                    info_type,
                    new ArrayList<>(Arrays.asList("info_type_id")),
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relational_val);
        }

        String movie_companies = "movie_companies";
        SchemaTableName movie_companies_schema = new SchemaTableName(testSchema, movie_companies);
        if (runtimeContext.getJdbcClient().getTableHandle(movie_companies_schema) == null) {
            List<List<RelationalValue>> relational_val = new ArrayList<>(List.of(
                    Arrays.asList(new IntegerValue(1),
                            new IntegerValue(1),
                            new IntegerValue(1))));
            runtimeContext.getJdbcClient().ingestRelation(
                    testSchema,
                    movie_companies,
                    new ArrayList<>(Arrays.asList("movie_id", "company_id", "company_type_id")),
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER, INTEGER)),
                    relational_val);
        }

        String movie_info_idx = "movie_info_idx";
        SchemaTableName movie_info_idx_schema = new SchemaTableName(testSchema, movie_info_idx);
        if (runtimeContext.getJdbcClient().getTableHandle(movie_info_idx_schema) == null) {
            List<List<RelationalValue>> relational_val = new ArrayList<>(List.of(
                    Arrays.asList(new IntegerValue(1),
                            new IntegerValue(1))));
            runtimeContext.getJdbcClient().ingestRelation(
                    testSchema,
                    movie_info_idx,
                    new ArrayList<>(Arrays.asList("movie_id", "info_type_id")),
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                    relational_val);
        }

        String title = "title";
        SchemaTableName title_schema = new SchemaTableName(testSchema, title);
        if (runtimeContext.getJdbcClient().getTableHandle(title_schema) == null) {
            List<List<RelationalValue>> relational_val = new ArrayList<>(List.of(
                    Arrays.asList(new IntegerValue(1),
                            new IntegerValue(1))));
            runtimeContext.getJdbcClient().ingestRelation(
                    testSchema,
                    title,
                    new ArrayList<>(Arrays.asList("movie_id", "kind_id")),
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                    relational_val);
        }
    }

    @Test
    public void jobQ1a()
    {
        String sql = "SELECT MIN(mc.note) AS production_note,\n" +
                "       MIN(t.title) AS movie_title,\n" +
                "       MIN(t.production_year) AS movie_year\n" +
                "FROM company_type AS ct,\n" +
                "     info_type AS it,\n" +
                "     movie_companies AS mc,\n" +
                "     movie_info_idx AS mi_idx,\n" +
                "     title AS t\n" +
                "WHERE ct.kind = 'production companies'\n" +
                "  AND it.info = 'top 250 rank'\n" +
                "  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'\n" +
                "  AND (mc.note LIKE '%(co-production)%'\n" +
                "       OR mc.note LIKE '%(presents)%')\n" +
                "  AND ct.id = mc.company_type_id\n" +
                "  AND t.id = mc.movie_id\n" +
                "  AND t.id = mi_idx.movie_id\n" +
                "  AND mc.movie_id = mi_idx.movie_id\n" +
                "  AND it.id = mi_idx.info_type_id";
        SqlNode root = parser.createStatement(sql);
        MultiwayJoinGraph queryGraph = createQueryGraph(root, runtimeContext);
        Set<MultiwayJoinNode> nodesSet = queryGraph.getNodes();
        assertEquals(nodesSet.size(), 5);

        for (BaseNode baseNode : nodesSet) {
            String connectedString = baseNode.connectedToString();
            String nodeName = baseNode.getNodeName();
            if (nodeName.contains("company_type")) {
                assertEquals(1, baseNode.getConnected().size());
                assertTrue(connectedString.contains("movie_companies"));
            }
            else if (nodeName.contains("movie_companies")) {
                assertEquals(3, baseNode.getConnected().size());
                assertTrue(connectedString.contains("title"));
                assertTrue(connectedString.contains("movie_info_idx"));
            }
            else if (nodeName.contains("title")) {
                assertEquals(2, baseNode.getConnected().size());
                assertTrue(connectedString.contains("movie_companies"));
                assertTrue(connectedString.contains("movie_info_idx"));
            }
            else if (nodeName.contains("movie_info_idx")) {
                assertEquals(3, baseNode.getConnected().size());
                assertTrue(connectedString.contains("movie_companies"));
                assertTrue(connectedString.contains("title"));
            }
            else if (nodeName.contains("info_type")) {
                assertEquals(1, baseNode.getConnected().size());
                assertTrue(connectedString.contains("movie_info_idx"));
            }
            else {
                throw new TreeTrackerException(INVALID_USAGE, "Unknown nodeName: " + nodeName);
            }
        }
    }
}
