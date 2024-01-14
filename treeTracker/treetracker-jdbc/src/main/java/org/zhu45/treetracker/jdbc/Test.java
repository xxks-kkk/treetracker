package org.zhu45.treetracker.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class Test
{
    public static void main(String[] args)
            throws ClassNotFoundException, SQLException
    {
//        Class.forName("org.duckdb.DuckDBDriver");
//        Connection conn = DriverManager.getConnection("jdbc:duckdb:");
//        Statement stmt = conn.createStatement();
//        stmt.executeQuery("SELECT 42");
//        stmt.execute("CREATE SCHEMA IF NOT EXISTS \"memory\".\"public\"");
//        stmt.execute("CREATE TABLE \"memory\".\"public\".\"L1\" (\"color\" char(10) NOT NULL)");
//        ResultSet resultSet = stmt.executeQuery("SELECT * from memory.public.L1");
//        System.out.println(resultSet);

//        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
//                Statement stmt = conn.createStatement();) {
//            stmt.execute("CREATE SCHEMA IF NOT EXISTS \"memory\".\"public\"");
//        }
//
//        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
//                Statement stmt = conn.createStatement();) {
//            stmt.execute("CREATE TABLE \"memory\".\"public\".\"L1\" (\"color\" char(10) NOT NULL)");
//        }

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:/home/zeyuanhu/projects/challenge-set-gitlab/treeTracker/treetracker-jdbc/main.duckdb")) {
            DatabaseMetaData metadata = connection.getMetaData();
            System.out.println("catalog: " + connection.getCatalog());
            ResultSet resultSet = metadata.getTables(
                    connection.getCatalog(),
                    "imdb_int",
                    "cast_info",
                    new String[] {"BASE TABLE", "VIEW"});
            boolean hasNext = resultSet.next();
            System.out.println(hasNext);
        }
    }
}
