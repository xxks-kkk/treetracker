package org.zhu45.treetracker.common.row;

import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;

/**
 * A special kind of Row where all the values in it are integers.
 */
public class IntRow
        extends AbstractRow
{
    private static final RowType rowType = RowType.INTROW;

    int[] vals;

    public IntRow(List<String> attributes, int[] vals)
    {
        this.attributes = attributes;
        // We convert this to List when performing join (increase join overhead)
        this.vals = vals;
        this.types = Collections.nCopies(attributes.size(), INTEGER);
    }

    @Override
    public List<RelationalValue> getVals()
    {
        return Arrays.stream(vals).mapToObj(IntegerValue::new).collect(Collectors.toList());
    }

    public int[] getIntVals()
    {
        // TODO: need to use this method in extract() instead of getVals
        return vals;
    }

    @Override
    public int size()
    {
        return vals.length;
    }

    @Override
    public RowType getRowType()
    {
        return rowType;
    }

    @Override
    public Iterator<RelationalValue> iterator()
    {
        return Arrays.stream(this.vals).mapToObj(value -> (RelationalValue) new IntegerValue(value)).collect(Collectors.toList()).iterator();
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
        IntRow o = (IntRow) obj;
        // NOTE: potentially, this causes bug as two rows from different schemas may be considered equal, e.g.,
        // for R(a int) and S(b int), we have R(5), S(5). They should not be the same because they have different attributes.
        // However, from the benchmarking TTJ perspective, the following implementation is sufficient because we always
        // compare rows from the same schema.
        return Arrays.equals(this.vals, o.vals);
    }

    @Override
    public int hashCode()
    {
        // NOTE: We may need to think about whether the following hashing provides an uniformly distributed of hash values,
        // which matters for the performance. For the current, when adding rows into hashset or hash table, we don't need to
        // hash types and attributes because rows from the same table will have the same hash values on types and attributes.
        // Thus, hashing vals is sufficient.
        return Arrays.hashCode(vals);
    }

    @Override
    public String toString()
    {
        return Arrays.toString(vals);
    }

    public ObjectRow toObjectRow()
    {
        return new ObjectRow(this.attributes, getVals());
    }
}
