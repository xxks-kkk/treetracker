package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.type.Type;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;

/**
 * In-memory representation of Column data of a relation. We use column-oriented storage.
 */
public class Column
        implements Iterable<RelationalValue>
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(Column.class);

    private String name;
    private Type type;
    // Contains data in the column
    private List<RelationalValue> vals;
    // Indicate whether the column can be null or not
    private boolean nullable;

    public Column(String name, Type type, List<RelationalValue> vals)
    {
        this.name = requireNonNull(name, "Column has to have a name");
        this.type = requireNonNull(type, "A type has to be specified for column");
        this.vals = requireNonNull(vals, "vals cannot be null");
    }

    public Type getType()
    {
        return type;
    }

    public List<RelationalValue> getVals()
    {
        return vals;
    }

    public String getName()
    {
        return name;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public int size()
    {
        return vals.size();
    }

    public void add(RelationalValue val)
    {
        this.vals.add(val);
    }

    @Override
    public Iterator<RelationalValue> iterator()
    {
        return this.vals.iterator();
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
        Column o = (Column) obj;
        log.debug(String.format("this: %s\no: %s\n", this, o));
        log.debug(String.format("this.vals equals o.vals: %s", Objects.equals(this.vals, o.vals)));
        log.debug(String.format("this.type equals o.type: %s", Objects.equals(this.type, o.type)));
        log.debug(String.format("this.name equals o.name: %s", Objects.equals(this.name, o.name)));
        return Objects.equals(this.vals, o.vals) &&
                Objects.equals(this.type, o.type) &&
                Objects.equals(this.name, o.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(vals, type, name);
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
        if (o1.getClass() != Column.class) {
            throw new TreeTrackerException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "cannot be applied to non Column object");
        }
        Column column1 = (Column) o1;
        Column column2 = (Column) o2;
        log.debug(String.format("o1: %s\no2: %s\n", column1, column2));
        log.debug(String.format("o1.vals equals o2.vals: %s\n", Utils.collectionCompare(column1.vals, column2.vals, Object::equals)));
        log.debug(String.format("o1.type equals o2.type: %s\n", Objects.equals(column1.type, column2.type)));
        log.debug(String.format("o1.name equals o2.name: %s\n", Objects.equals(column1.name, column2.name)));
        // This function is used mainly for natural join result check.
        // Since in that scenario, we only want to find the column with the same content, we don't care if the column name is different.
        return Utils.collectionCompare(column1.vals, column2.vals, Object::equals) &&
                Objects.equals(column1.type, column2.type);
    }
}
