package org.zhu45.treetracker.common;

import de.renebergelt.test.Switches;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.commons.collections4.CollectionUtils.isEqualCollection;

/**
 * BaseDomain contains a set of Values, which a node in CSP graph
 * can be assigned to
 */
public class BaseDomain<T extends Domain<T, V>, V extends Value>
        implements Domain<T, V>
{
    private final MultiSet<V> domain;

    @SafeVarargs
    public BaseDomain(V... vals)
    {
        this.domain = new HashMultiSet<>();
        this.domain.addAll(List.of(vals));
    }

    public BaseDomain(BaseDomain<T, V> other)
    {
        this.domain = new HashMultiSet<>(other.domain);
    }

    @Override
    public Domain<T, V> deepCopy()
    {
        return new BaseDomain<>(this);
    }

    @Override
    public List<V> getDomainAsList()
    {
        return new ArrayList<>(this.domain);
    }

    @Override
    public boolean isEmpty()
    {
        return domain.isEmpty();
    }

    @Override
    public Iterator<V> iterator()
    {
        return domain.iterator();
    }

    /**
     * Updates domain by remove given v from the domain
     *
     * @param v value to be removed
     * @return true if remove is successful; false otherwise
     */
    @Override
    public boolean remove(V v)
    {
        return domain.remove(v);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (!(o instanceof BaseDomain)) {
            return false;
        }
        BaseDomain<T, V> c = (BaseDomain<T, V>) o;
        return isEqualCollection(c.getDomainAsList(), this.getDomainAsList());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(domain);
    }

    @Override
    public boolean add(V val)
    {
        return this.domain.add(val);
    }

    @Override
    public void clear()
    {
        domain.clear();
    }

    @Override
    public String toString()
    {
        return this.domain.toString();
    }

    @Override
    public int size()
    {
        return this.domain.size();
    }

    public void setDomainVals(List<V> vals)
    {
        this.domain.clear();
        if (Switches.DEBUG) {
            checkArgument(domain.isEmpty(), "domain is not cleared");
        }
        this.domain.addAll(vals);
    }
}
