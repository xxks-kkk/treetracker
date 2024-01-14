package org.zhu45.treetracker.relational.planner.rule;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.checkIfCrossProductsFreeOrderingExists;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoinOrdering
{
    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestJoinOrdering";

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(JoinOrdering.class.getName(), Level.DEBUG);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
    }

    private Object[][] testCheckIfCrossProductsFreeOrderingExistsDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestCheckIfCrossProductsFreeOrderingExistsCases.class)));
    }

    public static class TestCheckIfCrossProductsFreeOrderingExistsCases
            implements TestCases
    {
        private String schema = "TestCheckIfCrossProductsFreeOrderingExistsCases";

        public TestCheckIfCrossProductsFreeOrderingExistsCases(TestingPhysicalPlanBase base)
        {
        }

        public Triple<Collection<SchemaTableName>, HashMap<SchemaTableName, List<String>>, Boolean> ordering1()
        {
            SchemaTableName mii2 = new SchemaTableName(schema, "mii2");
            SchemaTableName it2 = new SchemaTableName(schema, "it2");
            SchemaTableName ml = new SchemaTableName(schema, "ml");
            SchemaTableName lt = new SchemaTableName(schema, "lt");
            SchemaTableName t2 = new SchemaTableName(schema, "t2");
            SchemaTableName kt2 = new SchemaTableName(schema, "kt2");
            SchemaTableName mii1 = new SchemaTableName(schema, "mii1");
            SchemaTableName it1 = new SchemaTableName(schema, "it1");
            SchemaTableName mc2 = new SchemaTableName(schema, "mc2");
            SchemaTableName t1 = new SchemaTableName(schema, "t1");
            SchemaTableName kt1 = new SchemaTableName(schema, "kt1");
            SchemaTableName mc1 = new SchemaTableName(schema, "mc1");
            SchemaTableName cn1 = new SchemaTableName(schema, "cn1");
            SchemaTableName cn2 = new SchemaTableName(schema, "cn2");

            List<SchemaTableName> schemaTableNames = List.of(mii2, it2, ml, lt, t2, kt2, mii1, it1, mc2, t1, kt1, mc1, cn1, cn2);

            HashMap<SchemaTableName, List<String>> schemaTableName2Attributes = new HashMap<>();
            schemaTableName2Attributes.put(mii2, List.of("linked_movie_id", "info_type_id2"));
            schemaTableName2Attributes.put(it2, List.of("info_type_id2"));
            schemaTableName2Attributes.put(ml, List.of("movie_id", "linked_movie_id", "link_type_id"));
            schemaTableName2Attributes.put(lt, List.of("link_type_id"));
            schemaTableName2Attributes.put(t2, List.of("linked_movie_id", "kind_id2"));
            schemaTableName2Attributes.put(kt2, List.of("kind_id2"));
            schemaTableName2Attributes.put(mii1, List.of("movie_id", "info_type_id"));
            schemaTableName2Attributes.put(it1, List.of("info_type_id"));
            schemaTableName2Attributes.put(mc2, List.of("linked_movie_id", "company_id2", "company_type_id2"));
            schemaTableName2Attributes.put(t1, List.of("movie_id", "kind_id"));
            schemaTableName2Attributes.put(kt1, List.of("kind_id"));
            schemaTableName2Attributes.put(mc1, List.of("movie_id", "company_id", "company_type_id"));
            schemaTableName2Attributes.put(cn1, List.of("company_id"));
            schemaTableName2Attributes.put(cn2, List.of("company_id2"));

            return Triple.of(schemaTableNames, schemaTableName2Attributes, true);
        }

        public Triple<Collection<SchemaTableName>, HashMap<SchemaTableName, List<String>>, Boolean> ordering2()
        {
            SchemaTableName ml = new SchemaTableName(schema, "ml");
            SchemaTableName mc2 = new SchemaTableName(schema, "mc2");
            SchemaTableName mc1 = new SchemaTableName(schema, "mc1");

            List<SchemaTableName> schemaTableNames = List.of(ml, mc2, mc1);

            HashMap<SchemaTableName, List<String>> schemaTableName2Attributes = new HashMap<>();
            schemaTableName2Attributes.put(ml, List.of("movie_id", "linked_movie_id", "link_type_id"));
            schemaTableName2Attributes.put(mc2, List.of("linked_movie_id", "company_id2", "company_type_id2"));
            schemaTableName2Attributes.put(mc1, List.of("movie_id", "company_id", "company_type_id"));

            return Triple.of(schemaTableNames, schemaTableName2Attributes, true);
        }

        public Triple<Collection<SchemaTableName>, HashMap<SchemaTableName, List<String>>, Boolean> ordering3()
        {
            SchemaTableName it2 = new SchemaTableName(schema, "it2");
            SchemaTableName ml = new SchemaTableName(schema, "ml");
            SchemaTableName lt = new SchemaTableName(schema, "lt");
            SchemaTableName t2 = new SchemaTableName(schema, "t2");
            SchemaTableName kt2 = new SchemaTableName(schema, "kt2");
            SchemaTableName mii1 = new SchemaTableName(schema, "mii1");
            SchemaTableName it1 = new SchemaTableName(schema, "it1");
            SchemaTableName mc2 = new SchemaTableName(schema, "mc2");
            SchemaTableName t1 = new SchemaTableName(schema, "t1");
            SchemaTableName kt1 = new SchemaTableName(schema, "kt1");
            SchemaTableName mc1 = new SchemaTableName(schema, "mc1");
            SchemaTableName cn1 = new SchemaTableName(schema, "cn1");
            SchemaTableName cn2 = new SchemaTableName(schema, "cn2");

            List<SchemaTableName> schemaTableNames = List.of(it2, ml, lt, t2, kt2, mii1, it1, mc2, t1, kt1, mc1, cn1, cn2);

            HashMap<SchemaTableName, List<String>> schemaTableName2Attributes = new HashMap<>();
            schemaTableName2Attributes.put(it2, List.of("info_type_id2"));
            schemaTableName2Attributes.put(ml, List.of("movie_id", "linked_movie_id", "link_type_id"));
            schemaTableName2Attributes.put(lt, List.of("link_type_id"));
            schemaTableName2Attributes.put(t2, List.of("linked_movie_id", "kind_id2"));
            schemaTableName2Attributes.put(kt2, List.of("kind_id2"));
            schemaTableName2Attributes.put(mii1, List.of("movie_id", "info_type_id"));
            schemaTableName2Attributes.put(it1, List.of("info_type_id"));
            schemaTableName2Attributes.put(mc2, List.of("linked_movie_id", "company_id2", "company_type_id2"));
            schemaTableName2Attributes.put(t1, List.of("movie_id", "kind_id"));
            schemaTableName2Attributes.put(kt1, List.of("kind_id"));
            schemaTableName2Attributes.put(mc1, List.of("movie_id", "company_id", "company_type_id"));
            schemaTableName2Attributes.put(cn1, List.of("company_id"));
            schemaTableName2Attributes.put(cn2, List.of("company_id2"));

            return Triple.of(schemaTableNames, schemaTableName2Attributes, false);
        }

        public Triple<Collection<SchemaTableName>, HashMap<SchemaTableName, List<String>>, Boolean> ordering4()
        {
            SchemaTableName ml = new SchemaTableName(schema, "ml");
            SchemaTableName cn2 = new SchemaTableName(schema, "cn2");

            List<SchemaTableName> schemaTableNames = List.of(ml, cn2);

            HashMap<SchemaTableName, List<String>> schemaTableName2Attributes = new HashMap<>();
            schemaTableName2Attributes.put(ml, List.of("movie_id", "linked_movie_id", "link_type_id"));
            schemaTableName2Attributes.put(cn2, List.of("company_id2"));

            return Triple.of(schemaTableNames, schemaTableName2Attributes, false);
        }
    }

    @ParameterizedTest
    @MethodSource("testCheckIfCrossProductsFreeOrderingExistsDataProvider")
    public void testCheckIfCrossProductsFreeOrderingExists(Triple<Collection<SchemaTableName>, HashMap<SchemaTableName, List<String>>, Boolean> triple)
    {
        assertEquals(triple.getRight(),
                checkIfCrossProductsFreeOrderingExists(triple.getLeft(),
                        triple.getMiddle()));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
