package org.zhu45.treetracker.jdbc.testing;

import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.zhu45.treetracker.common.Column;
import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.zhu45.treetracker.common.Utils.collectionCompare;

public class TestUtils
{
    private TestUtils() {}

    public static Properties getProperties(String filename)
    {
        Properties props = new Properties();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream(filename);
        try {
            props.load(is);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not load unittest.properties", e);
        }
        return props;
    }

    public static String getCurrentMethod()
    {
        return getCurrentMethod(1);
    }

    private static String getCurrentMethod(int skip)
    {
        return Thread.currentThread().getStackTrace()[1 + 1 + skip].getMethodName();
    }

    public static Object[][] twoDlistTo2DArray(List<List<Object>> tmp)
    {
        return tmp.stream().map(u -> u.toArray(new Object[0])).toArray(Object[][]::new);
    }

    public static boolean columnCompare(MultiSet<Row> actual, MultiSet<Row> expected)
    {
        if (expected.size() != actual.size()) {
            return false;
        }
        Relation expectedAsRelation = new Relation(new RowSet(new ArrayList<>(expected)));
        Relation actualAsRelation = new Relation(new RowSet(new ArrayList<>(actual)));
        // In this comparison, we ignore the row ordering. This can lead to false positive:
        // incorrect actual result set is marked "the same" as expected. So, we should never
        // use this method standalone as the result set checker. It is more for auxiliary checker
        // that boosts our confidence in the obtained result set. The main checker is rowCompare function.
        List<Column> expectedColumns = expectedAsRelation.getColumns();
        List<Column> actualColumns = actualAsRelation.getColumns();
        return collectionCompare(expectedColumns, actualColumns, Column::equalsIgnoreOrder);
    }

    public static boolean rowCompare(MultiSet<Row> expected, MultiSet<Row> actual)
    {
        if (!actual.isEmpty()) {
            // This block handles the case of comparing a collection IntRows with a collection of ObjectRows
            if (actual.iterator().next().getClass() == IntRow.class) {
                MultiSet<ObjectRow> converted = new HashMultiSet<>(actual.stream().map(r -> ((IntRow) r).toObjectRow()).collect(Collectors.toList()));
                return collectionCompare(expected, converted, ObjectRow::equalsIgnoreOrder);
            }
        }
        // In this comparison, we ignore the column ordering. That is, Postgres result set
        // might have different column ordering than the one computed by TT joins. This depends
        // on the join order.
        return collectionCompare(expected, actual, ObjectRow::equalsIgnoreOrder);
    }
}
