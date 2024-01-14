package org.zhu45.treetracker.relational.parser;

import de.renebergelt.test.Switches;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.logging.LoggerProvider;

import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_USER_ERROR;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;

public class SqlParser
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(SqlParser.class);

    public SqlParser()
    {
    }

    public SqlNode createStatement(String sql)
    {
        // Parse the query into an AST
        org.apache.calcite.sql.parser.SqlParser parser = org.apache.calcite.sql.parser.SqlParser.create(sql);
        try {
            SqlNode sqlNode = parser.parseQuery();
            if (Switches.DEBUG) {
                log.debug("[Parsed query]\n:" + sqlNode.toString());
            }
            return sqlNode;
        }
        catch (SqlParseException e) {
            throw new TreeTrackerException(GENERIC_USER_ERROR, e);
        }
    }
}
