package org.zhu45.treetracker.relational.operator.testCases;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treektracker.multiwayJoin.testing.TestingTreeTrackerJoinComplexCases;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;

/**
 * Instantiate test cases for all operator/execution tests.
 * <p>
 * TODO: we can enhance this class by allowing user to specify test cases from what classes
 * can be run. Right now, we blindly construct all test cases to consume. However, for some
 * operator, there exists some specific assumptions (e.g., TTJ operator assumes to work with
 * left-deep query plan). Thus, tests for those operators are using filters to filter out
 * test cases they don't want to run (see TestTupleBaseTreeTrackerOneBetaHashTableOperator as an example).
 */
public class InstantiateTestCases
{
    private InstantiateTestCases()
    {
    }

    private static final LoggerProvider.TreeTrackerLogger log = getLogger(InstantiateTestCases.class);

    public static List<List<Object>> buildAllTestCases(TestingPhysicalPlanBase base)
    {
        TestCaseBuilder builder = new TestCaseBuilder(base, Optional.empty());
        List<List<Object>> cases = new ArrayList<>();
        cases.addAll(builder.instantiateTestCases());
        // Add test cases generated from constructing random join trees
        cases.addAll(builder.generateTestCasesFromRandomJoinTrees());
        // Add test cases from TestingTreeTrackerJoinComplexCases
        cases.addAll(builder.addCasesFromTestingTreeTrackerJoinComplexCases());
        return cases;
    }

    public static List<List<Object>> buildSpecificTestCases(TestingPhysicalPlanBase base,
                                                            List<Class<? extends TestCases>> testCaseClazz)
    {
        TestCaseBuilder builder = new TestCaseBuilder(base, Optional.of(testCaseClazz));
        return new ArrayList<>(builder.instantiateTestCases());
    }

    public static class TestCaseBuilder
    {
        private List<Class<? extends TestCases>> testCaseClazz;
        private TestingPhysicalPlanBase base;

        public TestCaseBuilder(TestingPhysicalPlanBase base, Optional<List<Class<? extends TestCases>>> testCaseClazz)
        {
            this.base = base;
            testCaseClazz.ifPresentOrElse(
                    testcases -> this.testCaseClazz = testcases,
                    () -> this.testCaseClazz = List.of(
                            TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases.class,
                            TestTupleBasedTreeTrackerTwoOperatorCases.class)
            );
        }

        private List<List<Object>> instantiateTestCases()
        {
            try {
                List<List<Object>> res = new ArrayList<>();
                for (Class<? extends TestCases> testCaseClz : testCaseClazz) {
                    Constructor cons = testCaseClz.getConstructor(TestingPhysicalPlanBase.class);
                    TestCases cases = (TestCases) cons.newInstance(base);
                    Method[] methods = cases.getClass().getDeclaredMethods();
                    for (Method testCaseCreationMethod : methods) {
                        log.debug("method: " + testCaseCreationMethod);
                        List<Object> testCase = Collections.singletonList(testCaseCreationMethod.invoke(cases));
                        res.add(testCase);
                    }
                }
                return res;
            }
            catch (NoSuchMethodException noSuchMethodException) {
                throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, "Target constructor is not found");
            }
            catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
                throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, "Something wrong with instantiateTestCases() \n" + e);
            }
        }

        /**
         * A wrapper to test cases in TestingTreeTrackerJoinComplexCases so that we can reuse them in the operator and
         * execution testing.
         */
        public List<List<Object>> addCasesFromTestingTreeTrackerJoinComplexCases()
        {
            requireNonNull(base, "base is null");
            List<List<Object>> res = new ArrayList<>();
            List<List<MultiwayJoinNode>> nodesList = new ArrayList<>();
            try {
                TestingTreeTrackerJoinComplexCases cases = new TestingTreeTrackerJoinComplexCases(base.getDatabase());
                Method[] methods = cases.getClass().getDeclaredMethods();
                for (Method testCaseCreationMethod : methods) {
                    log.debug("method: " + testCaseCreationMethod);
                    Pair<MultiwayJoinGraph, MultiwayJoinNode> pair = (Pair<MultiwayJoinGraph, MultiwayJoinNode>) testCaseCreationMethod.invoke(cases);
                    MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(pair.getKey(), pair.getValue());
                    nodesList.add(orderedGraph.getTraversalList());
                    res.add(Collections.singletonList(base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                            Optional.empty(),
                            Optional.empty())));
                }
                return res;
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new TreeTrackerException(GENERIC_INTERNAL_ERROR,
                        "error in testingTreeTrackerJoinComplexCasesToTestingOperatorExecutionComplexCases");
            }
            finally {
                nodesList.forEach(nodes -> {
                    for (MultiwayJoinNode node : nodes) {
                        node.getDomain().close();
                    }
                });
            }
        }

        private List<List<Object>> generateTestCasesFromRandomJoinTrees()
        {
            List<List<Object>> tmp = new ArrayList<>();
            for (int i = 2; i < base.getDatabase().getNumRelations() + 1; ++i) {
                Pair<MultiwayJoinOrderedGraph, MultiwayJoinNode> pair = ((TestingMultiwayJoinDatabaseComplex) base.getDatabase()).createJoinTree(i);
                tmp.add(Collections.singletonList(base.createFixedPhysicalPlanFromQueryGraph(pair.getKey(),
                        Optional.empty(),
                        Optional.empty())));
            }
            return tmp;
        }
    }
}
