package org.zhu45.treetracker.jdbc;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class JdbcConnectorId
{
    private final String id;

    public JdbcConnectorId(String id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    @Override
    public String toString()
    {
        return id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
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
        JdbcConnectorId other = (JdbcConnectorId) obj;
        return Objects.equals(this.id, other.id);
    }
}
