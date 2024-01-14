package org.zhu45.treetracker.jdbc;

public class JdbcIdentity
{
    private String connectionUser;
    private String connectionPassword;

    public String getConnectionPassword()
    {
        return connectionPassword;
    }

    public String getConnectionUser()
    {
        return connectionUser;
    }

    public JdbcIdentity(String connectionUser, String connectionPassword)
    {
        this.connectionUser = connectionUser;
        this.connectionPassword = connectionPassword;
    }
}
