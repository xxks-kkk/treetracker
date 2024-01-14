package org.zhu45.treetracker.common.row;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.JoinInfo;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.StandardErrorCode;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.zhu45.treetracker.common.Utils.collectionCompare;

public class ObjectRow
        extends AbstractRow
{
    private static final Logger traceLogger;
    private static final RowType roleType = RowType.OBJECTROW;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(ObjectRow.class);
        }
        else {
            traceLogger = null;
        }
    }

    private final List<RelationalValue> vals;

    public ObjectRow(List<String> attributes, List<RelationalValue> vals)
    {
        this.attributes = attributes;
        this.vals = vals;
        this.types = vals.stream().map(RelationalValue::getType).collect(Collectors.toList());
        // Intentionally, RowId is uninitialized here because RowId is not used by TTJ (i.e., the creation of
        // RowId is legacy where we first introduce it to solve issues in monolithic TTJ; ideally, we want
        // to remove RowId completely, i.e., whether monolithic TTJ still needs it is questionable). However,
        // during TTJ computation, whenever a ObjectRow is created (intermediate result), an unneeded RowId is created as well,
        // which takes memory space and does nothing. Ideally, getNextRow() in RecordTupleSource doesn't need to initialize RowId
        // for TTJ. However, since the code is shared with monolithic TTJ and each row from each table is read once,
        // we decide to pay the cost.
    }

    /**
     * NOTE: we can calculate types but sometimes, e.g., RecordTupleSource, types are available already and can be
     * passed in as an argument. Thus, we can avoid repeated calculation on types in construction.
     */
    public ObjectRow(List<String> attributes, List<Type> types, List<RelationalValue> vals)
    {
        this.attributes = attributes;
        this.vals = vals;
        this.types = types;
        // Intentionally, RowId is uninitialized here because RowId is not used by TTJ (i.e., the creation of
        // RowId is legacy where we first introduce it to solve issues in monolithic TTJ; ideally, we want
        // to remove RowId completely, i.e., whether monolithic TTJ still needs it is questionable). However,
        // during TTJ computation, whenever a ObjectRow is created (intermediate result), an unneeded RowId is created as well,
        // which takes memory space and does nothing. Ideally, getNextRow() in RecordTupleSource doesn't need to initialize RowId
        // for TTJ. However, since the code is shared with monolithic TTJ and each row from each table is read once,
        // we decide to pay the cost.
    }

    public ObjectRow()
    {
        this.attributes = new ArrayList<>();
        this.types = new ArrayList<>();
        this.vals = new ArrayList<>();
    }

    /**
     * Check two rows that have the same content (but they can have
     * different rowIds).
     */
    public static boolean equals(Object o1, Object o2)
    {
        if (o1 == o2) {
            return true;
        }
        if ((o1 == null) || o2 == null) {
            return false;
        }
        if (o1.getClass() != o2.getClass()) {
            return false;
        }
        if (o1.getClass() != ObjectRow.class) {
            throw new TreeTrackerException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "cannot be applied to non Column object");
        }
        ObjectRow row1 = (ObjectRow) o1;
        ObjectRow row2 = (ObjectRow) o2;
        return row1.equals(row2);
    }

    public static boolean equalsIgnoreOrder(Object o1, Object o2)
    {
        if (o1 == o2) {
            return true;
        }
        if ((o1 == null) || (o2 == null)) {
            return false;
        }
        if (o1.getClass() != o2.getClass()) {
            return false;
        }
        if (o1.getClass() != ObjectRow.class) {
            throw new TreeTrackerException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "cannot be applied to non Column object");
        }
        ObjectRow row1 = (ObjectRow) o1;
        ObjectRow row2 = (ObjectRow) o2;
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(String.format("this: %s\no: %s\n", row1, row2));
            traceLogger.debug(String.format("this.vals: %s\no.vals: %s\n", row1.vals, row2.vals));
            traceLogger.debug(String.format("this.type: %s\no.type: %s\n", row1.attributes, row2.attributes));
            traceLogger.debug(String.format("this.name: %s\no.name: %s\n", row1.types, row2.types));
        }
        return collectionCompare(row1.vals, row2.vals, Object::equals) &&
                collectionCompare(row1.attributes, row2.attributes, Object::equals) &&
                collectionCompare(row1.types, row2.types, Object::equals);
    }

    public static ObjectRow join(ObjectRow one, ObjectRow other)
    {
        if (one.isEmpty()) {
            return other;
        }
        else if (other.isEmpty()) {
            return one;
        }
        else {
            Set<Integer> processedRelationSColumn = new HashSet<>();
            List<String> resultTupleAttributes = new ArrayList<>();
            List<Type> types = new ArrayList<>();
            List<String> oneAttributes = one.getAttributes();
            List<String> otherAttributes = other.getAttributes();
            List<Type> oneTypes = one.getTypes();
            List<Type> otherTypes = other.getTypes();
            Map<Integer, Integer> joinIdx = new HashMap<>();
            for (int i = 0; i < one.size(); ++i) {
                for (int j = 0; j < other.size(); ++j) {
                    if (oneAttributes.get(i).equals(otherAttributes.get(j)) && oneTypes.get(i).equals(otherTypes.get(j))) {
                        joinIdx.put(i, j);
                    }
                }
            }
            for (int i = 0; i < one.size(); ++i) {
                if (joinIdx.containsKey(i)) {
                    if (!one.entryEqual(other, i, joinIdx.get(i))) {
                        // Because we perform natural join and when the query graph doesn't satisfy the label-connectedness
                        // property, there is a chance that both rows have the same attribute but attribute corresponding
                        // values are different (e.g., both have the attribute "color" but one has value "grey" and the other
                        // has value "blue"). In this case, the join tuple should return null and it is caller's responsibility
                        // to check for non-null.
                        return null;
                    }
                    else {
                        processedRelationSColumn.add(joinIdx.get(i));
                    }
                }
                resultTupleAttributes.add(oneAttributes.get(i));
                types.add(oneTypes.get(i));
            }
            List<RelationalValue> res = new ArrayList<>(one.getVals());
            for (int i = 0; i < other.size(); ++i) {
                if (!processedRelationSColumn.contains(i)) {
                    resultTupleAttributes.add(otherAttributes.get(i));
                    types.add(otherTypes.get(i));
                    res.add(other.getVals().get(i));
                }
            }
            return new ObjectRow(resultTupleAttributes, res);
        }
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
        ObjectRow o = (ObjectRow) obj;
        // NOTE: potentially, this causes bug as two rows from different schemas may be considered equal, e.g.,
        // for R(a int) and S(b int), we have R(5), S(5). They should not be the same because they have different attributes.
        // However, from the benchmarking TTJ perspective, the following implementation is sufficient because we always
        // compare rows from the same schema.
        return Objects.equals(this.vals, o.vals);
    }

    @Override
    public int hashCode()
    {
        // NOTE: We may need to think about whether the following hashing provides an uniformly distributed of hash values,
        // which matters for the performance. For the current, when adding rows into hashset or hash table, we don't need to
        // hash types and attributes because rows from the same table will have the same hash values on types and attributes.
        // Thus, hashing vals is sufficient.
        return Objects.hash(vals);
    }

    @Override
    public String toString()
    {
        return String.valueOf(vals);
    }

    @Override
    public List<RelationalValue> getVals()
    {
        return vals;
    }

    @Override
    public Iterator<RelationalValue> iterator()
    {
        return this.vals.iterator();
    }

    @Override
    public int size()
    {
        return this.vals.size();
    }

    @Override
    public RowType getRowType()
    {
        return roleType;
    }

    /**
     * Checks whether the given row (other) can be joined with the current row (this)
     *
     * @param other
     */
    public JoinInfo joinable(ObjectRow other)
    {
        return new JoinInfo(this, other);
    }

    public boolean isEmpty()
    {
        return vals.isEmpty();
    }

    /**
     * Compare the given row's entry with the current row's entry to see both entries are equal
     *
     * @param other other row to compare
     * @param thisIndex entry of index in the current row
     * @param otherIndex entry of index in the given row
     * @return true if entries are equal; false otherwise
     */
    public boolean entryEqual(ObjectRow other, int thisIndex, int otherIndex)
    {
        List<String> otherAttributes = other.getAttributes();
        List<Type> otherTypes = other.getTypes();
        List<RelationalValue> otherVals = other.getVals();
        return (this.attributes.get(thisIndex).equals(otherAttributes.get(otherIndex))
                && this.types.get(thisIndex).equals(otherTypes.get(otherIndex))
                && this.vals.get(thisIndex).equals(otherVals.get(otherIndex)));
    }

    /**
     * Return the entry's attribute and type hash code.
     *
     * @param entryIdx entry index of the row
     * @return hash code based on entry's attribute and type
     */
    public int getEntryColumnHeaderHashCode(int entryIdx)
    {
        return new ColumnHeader(attributes.get(entryIdx), types.get(entryIdx)).hashCode();
    }

    private static class ColumnHeader
    {
        private final String attribute;
        private final Type type;

        public ColumnHeader(String attribute, Type type)
        {
            this.attribute = attribute;
            this.type = type;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(attribute, type);
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
            ColumnHeader o = (ColumnHeader) obj;
            return Objects.equals(this.attribute, o.attribute) &&
                    Objects.equals(this.type, o.type);
        }
    }
}
