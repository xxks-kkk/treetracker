package org.zhu45.treektracker.multiwayJoin.testing;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.ColorValue;
import org.zhu45.treetracker.common.Column;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.Value;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.testing.TestingDatabase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.StandardErrorCode.NOT_SUPPORTED;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

/**
 * This database creates relations will multiple attributes
 * with rows based on random sampling from prefixed domains.
 */
public class TestingMultiwayJoinDatabaseComplex
        extends TestingDatabase
{
    private static final LoggerProvider.TreeTrackerLogger log = getLogger(TestingMultiwayJoinDatabaseComplex.class);

    private static final CharType charType = createCharType(20);
    public static final String schemaName = "multiwaycomplex";
    private static final int MAX_NUM_ATTRIBUTES = 3;
    private static final int MAX_NUM_ROWS = 10;
    private Set<String> generatedRelationNames;
    // We want to make sure each relation has some common attribute(s) to join with.
    // The method we use doesn't enforce for sure this will happen with each pair of relation but the probability is high enough.
    private List<Attribute> commonAttributes;
    private Random rand;
    private Optional<Long> seed;
    private int numRelations;

    public enum Attribute
            implements Value
    {
        sex("sex"),
        name("name"),
        color("color"),
        age("age"),
        title("title");

        private final String attribute;

        Attribute(String attribute)
        {
            this.attribute = attribute;
        }

        public String getAttributeName()
        {
            return this.attribute;
        }

        public List<RelationalValue> getDomainForAttribute()
        {
            switch (attribute) {
                case "sex":
                    List<String> sex = new ArrayList<>(Arrays.asList(
                            "female", "male", "trans"));
                    return sex.stream().map(val -> new StringValue(charType, val)).collect(Collectors.toList());
                case "name":
                    List<String> names = new ArrayList<>(Arrays.asList(
                            "Liam", "Noah", "Oliver", "William", "Elijah",
                            "James", "Benjamin", "Lucas", "Mason", "Ethan",
                            "Olivia", "Emma", "Ava", "Sophia", "Isabella",
                            "Charlotte", "Amelia", "Mia", "Harper", "Evelyn"));
                    return names.stream().map(val -> new StringValue(charType, val)).collect(Collectors.toList());
                case "color":
                    List<ColorValue.Color> colors = new ArrayList<>(Arrays.asList(ColorValue.Color.values()));
                    return colors.stream().map(val -> new ColorValue(charType, val)).collect(Collectors.toList());
                case "age":
                    List<Integer> age = IntStream.rangeClosed(1, 20).boxed().collect(Collectors.toList());
                    return age.stream().map(String::valueOf).map(val -> new StringValue(charType, val)).collect(Collectors.toList());
                case "title":
                    List<String> titles = new ArrayList<>(Arrays.asList(
                            "Actuary", "Farmer", "Beekeeper", "Sales manager", "Barber",
                            "Cashier", "Singer", "Professor", "Researcher", "Architect",
                            "Librarian", "Petroleum engineer", "Credit analyst", "Pharmacist", "Housekeeper"));
                    return titles.stream().map(val -> new StringValue(charType, val)).collect(Collectors.toList());
                default:
                    throw new TreeTrackerException(NOT_SUPPORTED, "there is no matching domain for the given attribute");
            }
        }

        public List<RelationalValue> getRelationalValuesFromDomain(int numVal, Random rand)
        {
            List<RelationalValue> domain = getDomainForAttribute();
            List<RelationalValue> vals = new ArrayList<>();
            while (vals.size() < numVal) {
                RelationalValue val = domain.get(rand.nextInt(domain.size()));
                vals.add(val);
            }
            return vals;
        }
    }

    public TestingMultiwayJoinDatabaseComplex(JdbcClient jdbcClient, int numRelations, Optional<Long> seed)
            throws SQLException
    {
        super(jdbcClient);
        initialization(numRelations, seed);
    }

    private void initialization(int numRelations, Optional<Long> seed)
            throws SQLException
    {
        this.numRelations = numRelations;
        this.generatedRelationNames = new HashSet<>();
        this.rand = new Random();
        if (seed != null && seed.isPresent()) {
            this.seed = seed;
            rand.setSeed(seed.get());
        }
        else {
            this.seed = Optional.empty();
        }
        this.commonAttributes = new ArrayList<>();

        try (Connection connection = getJdbcClient().getConnection()) {
            log.debug("drop schema ...");
            connection.createStatement().execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", schemaName));
            log.debug("create schema ...");
            connection.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        createWholeRelations(numRelations);
    }

    private void createWholeRelations(int numRelations)
    {
        for (int i = 0; i < numRelations; ++i) {
            int numAttributes = rand.ints(1, 1, MAX_NUM_ATTRIBUTES).findFirst().getAsInt();
            int numRows = rand.ints(1, 1, MAX_NUM_ROWS).findFirst().getAsInt();
            createRelation(numAttributes, numRows);
        }
    }

    private void createRelation(int numAttributes, int numRows)
    {
        String relationName = createRandomRelationName();
        List<Column> columns = createColumns(numAttributes, numRows);
        Relation relation = new Relation(relationName, columns);

        // Expensive operation: only used for debug.
        log.info("relation: " + relationName);
        RowSet<Row> rowSet = new RowSet<>(relation.intoRowOriented());
        log.info(rowSet.renderOutput());

        // Since we already drop schema and recreate one in constructor, we don't need to check
        // for any pre-exist relations.
        getJdbcClient().createTable(schemaName, relation);
        getJdbcClient().insertTable(schemaName, relation);
    }

    private String createRandomRelationName()
    {
        String charLower = "abcdefghijklmnopqrstuvwxyz";
        String charUpper = charLower.toUpperCase();
        StringBuilder relationNameBuilder = new StringBuilder();
        while (true) {
            int rndCharAt = rand.nextInt(charUpper.length());
            relationNameBuilder.append(charLower.charAt(rndCharAt));
            if (!generatedRelationNames.contains(relationNameBuilder.toString())) {
                String relationName = relationNameBuilder.toString();
                generatedRelationNames.add(relationName);
                return relationName;
            }
        }
    }

    private List<Column> createColumns(int numAttributes, int numRows)
    {
        Attribute[] attributes = Attribute.values();
        List<Column> columns = new ArrayList<>();
        List<Attribute> attributeList = new ArrayList<>();
        do {
            double rndSamplingMethodWeight = rand.nextDouble();
            log.debug("rndSamplingMethodWeight: " + rndSamplingMethodWeight);
            Attribute attribute;
            if (rndSamplingMethodWeight > 0.5 || commonAttributes.size() == 0) {
                int rndPos = rand.nextInt(attributes.length);
                attribute = attributes[rndPos];
            }
            else {
                int rndPos = rand.nextInt(commonAttributes.size());
                attribute = commonAttributes.get(rndPos);
            }
            if (!attributeList.contains(attribute)) {
                attributeList.add(attribute);
                if (!commonAttributes.contains(attribute)) {
                    commonAttributes.add(attribute);
                }
            }
        }
        while (attributeList.size() != numAttributes);
        for (Attribute attribute : attributeList) {
            List<RelationalValue> vals = attribute.getRelationalValuesFromDomain(numRows, rand);
            columns.add(new Column(attribute.getAttributeName(), vals.get(0).getType(), vals));
        }
        return columns;
    }

    private Pair<List<Attribute>, List<Column>> createColumns2(int numAttributes, List<Attribute> pool, int numRows)
    {
        List<Column> columns = new ArrayList<>();
        List<Attribute> attributeList = new ArrayList<>();
        do {
            int rndPos = rand.nextInt(pool.size());
            Attribute attribute = pool.get(rndPos);
            if (!attributeList.contains(attribute)) {
                attributeList.add(attribute);
            }
        }
        while (attributeList.size() != numAttributes);
        for (Attribute attribute : attributeList) {
            List<RelationalValue> vals = attribute.getRelationalValuesFromDomain(numRows, rand);
            columns.add(new Column(attribute.getAttributeName(), vals.get(0).getType(), vals));
        }
        return Pair.of(attributeList, columns);
    }

    @Override
    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public List<SchemaTableName> getRelations()
    {
        List<SchemaTableName> relations = new ArrayList<>();
        for (String relationName : generatedRelationNames) {
            relations.add(new SchemaTableName(schemaName, relationName));
        }
        return relations;
    }

    @Override
    public Optional<Long> getSeed()
    {
        return this.seed;
    }

    /**
     * Create relations follow the tree structure, which is random generated
     * and has numRelations nodes. The resulting tree structure satisfies join tree property.
     * Return the join tree with its root.
     */
    public Pair<MultiwayJoinOrderedGraph, MultiwayJoinNode> createJoinTree(int numRelations)
    {
        HashMap<String, MultiwayJoinNode> multiwayJoinNodes = new HashMap<>();
        List<MultiwayJoinNode> joinTreeBaseNodes = new ArrayList<>();
        for (int i = 0; i < numRelations; ++i) {
            joinTreeBaseNodes.add(new MultiwayJoinNode(new SchemaTableName(schemaName, createRandomRelationName()), new MultiwayJoinDomain()));
        }
        Pair<MultiwayJoinOrderedGraph, MultiwayJoinNode> pair = generateARandomTree(joinTreeBaseNodes);
        Queue<MultiwayJoinNode> queue = new LinkedList<>();
        queue.add(pair.getValue());
        // stores the parent node's sampled attributes
        HashMap<MultiwayJoinNode, List<Attribute>> pool = new HashMap<>();
        pool.put(pair.getValue(), new ArrayList<>(Arrays.asList(Attribute.values())));
        int x = rand.ints(1, 1, pool.get(pair.getValue()).size()).findFirst().getAsInt();
        while (!queue.isEmpty()) {
            int l = queue.size();
            int i = 0;
            while (i < l) {
                MultiwayJoinNode head = queue.poll();
                int numRows = rand.ints(1, 1, MAX_NUM_ROWS).findFirst().getAsInt();
                requireNonNull(head, "head node is null");
                List<Attribute> currentNodePool = (head.equals(pair.getValue())) ? pool.get(head) : pool.get(pair.getKey().getParent().get(head).get(0));
                requireNonNull(currentNodePool, "currentNodePool is null");
                Pair<List<Attribute>, List<Column>> columns = createColumns2(x, currentNodePool, numRows);
                pool.put(head, columns.getKey());
                Relation relation = new Relation(head.getNodeName(), columns.getValue());

                // Expensive operation: only used for debug.
                log.info("relation: " + head.getNodeName());
                RowSet rowSet = new RowSet(relation.intoRowOriented());
                log.info(rowSet.renderOutput());

                // Since we already drop schema and recreate one in constructor, we don't need to check
                // for any pre-exist relations.
                getJdbcClient().createTable(schemaName, relation);
                getJdbcClient().insertTable(schemaName, relation);

                SchemaTableName schemaTableName = new SchemaTableName(schemaName, head.getNodeName());
                MultiwayJoinDomain domain = new MultiwayJoinDomain();
                MultiwayJoinNode node = new MultiwayJoinNode(schemaTableName, domain);
                multiwayJoinNodes.put(head.getNodeName(), node);

                queue.addAll(pair.getKey().getChildren().get(head));

                i++;
            }
            x = Math.max(x - rand.nextInt(x), 1);
        }
        return initializePair(multiwayJoinNodes, pair);
    }

    private Pair<MultiwayJoinOrderedGraph, MultiwayJoinNode> initializePair(HashMap<String, MultiwayJoinNode> multiwayJoinNodes,
            Pair<MultiwayJoinOrderedGraph, MultiwayJoinNode> pair)
    {
        List<Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeLists = new ArrayList<>();
        Queue<MultiwayJoinNode> queue = new LinkedList<>();
        queue.add(pair.getValue());
        MultiwayJoinNode root = multiwayJoinNodes.get(pair.getValue().getNodeName());
        while (!queue.isEmpty()) {
            MultiwayJoinNode head = queue.poll();
            MultiwayJoinNode multiwayJoinNode = multiwayJoinNodes.get(head.getNodeName());
            for (MultiwayJoinNode child : pair.getKey().getChildren().get(head)) {
                edgeLists.add(asEdge(multiwayJoinNode, multiwayJoinNodes.get(child.getNodeName())));
                queue.add(child);
            }
        }
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, root);
        log.debug("query graph:\n" + orderedGraph);
        Assertions.assertTrue(new CheckLabelConnectedness(orderedGraph.getTraversalList(), getJdbcClient()).check(),
                "resulting query graph doesn't satisfy join tree property.");
        return Pair.of(orderedGraph, root);
    }

    private Pair<MultiwayJoinOrderedGraph, MultiwayJoinNode> generateARandomTree(List<MultiwayJoinNode> baseNodes)
    {
        List<MultiwayJoinNode> s = new ArrayList<>();
        int numNodes = baseNodes.size();
        List<Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeLists = new ArrayList<>();
        MultiwayJoinNode root = baseNodes.get(rand.nextInt(baseNodes.size()));
        s.add(root);
        baseNodes.remove(root);
        for (int i = 1; i < numNodes; ++i) {
            MultiwayJoinNode baseNode1 = baseNodes.get(rand.nextInt(baseNodes.size()));
            MultiwayJoinNode baseNode2 = s.get(rand.nextInt(s.size()));
            edgeLists.add(asEdge(baseNode1, baseNode2));
            log.debugGreen(String.format("add edge: %s - %s", baseNode1, baseNode2));
            s.add(baseNode1);
            baseNodes.remove(baseNode1);
        }
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, root);
        assertEquals(orderedGraph.getTraversalList().size(), numNodes);
        return Pair.of(orderedGraph, root);
    }

    @Override
    public int getNumRelations()
    {
        return numRelations;
    }
}
