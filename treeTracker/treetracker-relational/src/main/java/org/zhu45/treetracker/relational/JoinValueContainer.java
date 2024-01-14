package org.zhu45.treetracker.relational;

import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.Value;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * This class is a container of the values under the join attributes. For example,
 * suppose we have relation R(a,b,c) and S(b,c). The join attributes is {b,c}.
 * Suppose we have R(1,2,3). A JoinValueContainer instance is [b:2, c:3]. This class
 * is used in combination with TT operator: goood list and no-good list.
 */
public class JoinValueContainer
        implements Value, Serializable
{
    private Map<String, RelationalValue> container;

    public JoinValueContainer()
    {
        this.container = UnifiedMap.newMap();
    }

    @Override
    public String toString()
    {
        return container.toString();
    }

    public Set<String> getJoinAttributeNames()
    {
        return container.keySet();
    }

    public Collection<RelationalValue> getValues()
    {
        return container.values();
    }

    public RelationalValue getValue(String attributeName)
    {
        return container.get(attributeName);
    }

    public int size()
    {
        return container.size();
    }

    public RelationalValue put(String key, RelationalValue val)
    {
        return container.put(key, val);
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
        JoinValueContainer o = (JoinValueContainer) obj;
        if (o.size() != this.size()) {
            return false;
        }
        return this.container.equals(o.container);
    }

    @Override
    public int hashCode()
    {
        return container.hashCode();
    }
}
