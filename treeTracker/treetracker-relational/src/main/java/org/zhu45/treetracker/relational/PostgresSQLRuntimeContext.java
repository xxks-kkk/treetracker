package org.zhu45.treetracker.relational;

import org.zhu45.treetracker.jdbc.JdbcClient;

public class PostgresSQLRuntimeContext
        implements RuntimeContext
{
    private JdbcClient jdbcClient;
    private String schema;

    private PostgresSQLRuntimeContext(Builder builder)
    {
        this.jdbcClient = builder.jdbcClient;
        this.schema = builder.schema;
    }

    public JdbcClient getJdbcClient()
    {
        return this.jdbcClient;
    }

    public String getSchema()
    {
        return schema;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        JdbcClient jdbcClient;
        String schema;

        public Builder()
        {
        }

        public Builder setJdbcClient(JdbcClient jdbcClient)
        {
            this.jdbcClient = jdbcClient;
            return this;
        }

        public Builder setSchema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public PostgresSQLRuntimeContext build()
        {
            return new PostgresSQLRuntimeContext(this);
        }
    }
}
