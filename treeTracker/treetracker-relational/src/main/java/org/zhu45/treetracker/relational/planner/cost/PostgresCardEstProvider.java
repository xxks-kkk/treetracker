package org.zhu45.treetracker.relational.planner.cost;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcSupplier;
import org.zhu45.treetracker.jdbc.PostgreSqlClient;
import org.zhu45.treetracker.jdbc.testing.NaturalJoinQueryBuilder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;

/**
 * Cardinality estimate is provided by Postgres EXPLAIN command
 */
public class PostgresCardEstProvider
        implements CardEstProvider
{
    private static final Logger traceLogger;
    private final boolean useTrueCard;

    public PostgresCardEstProvider()
    {
        useTrueCard = false;
    }

    public PostgresCardEstProvider(boolean useTrueCard)
    {
        this.useTrueCard = useTrueCard;
    }

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(PostgresCardEstProvider.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @Override
    public CardEstReturn getSize(Set<SchemaTableName> schemaTableNames, CardEstContext context)
    {
        // We don't need to enforce all join operations are hash join because the cardinality estimate
        // doesn't depend on the join method
        return CardEstReturn.builder(extractEstimateFromPlan(requireNonNull(getExplainPlan(schemaTableNames)), useTrueCard))
                .build();
    }

    @Override
    public CardEstType getCardEstType()
    {
        return CardEstType.NONTTJ;
    }

    @Override
    public boolean isUseTrueCard()
    {
        return useTrueCard;
    }

    private JsonArray getExplainPlan(Collection<SchemaTableName> schemaTableNames)
    {
        PostgreSqlClient postgresJdbcClient = (PostgreSqlClient) JdbcSupplier.postgresJdbcClientSupplier.get();
        try (Connection connection = postgresJdbcClient.getConnection()) {
            PreparedStatement preparedStatement = new NaturalJoinQueryBuilder(postgresJdbcClient.getIdentifierQuote()).buildExplainSql(
                    postgresJdbcClient,
                    connection,
                    schemaTableNames,
                    useTrueCard);
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug(preparedStatement.toString());
            }
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    String rawJsonString = resultSet.getString(1);
                    return JsonParser.parseString(rawJsonString).getAsJsonArray();
                }
            }
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
        return null;
    }

    public static long extractEstimateFromPlan(JsonArray jsonArray, boolean useTrueCard)
    {
        // NOTE: We assume the plan generated using SELECT * (or equivalent spelled-out form), i.e., the
        // SQL should not use count(*), which will make the return of this function doesn't make sense.
        // Ideally, we want to check if the assumption is true inside Postgres plan, but I cannot find a way to achieve it.
        JsonObject rootNode = jsonArray.get(0).getAsJsonObject().get("Plan").getAsJsonObject();
        if (useTrueCard) {
            return rootNode.get("Actual Rows").getAsLong();
        }
        return rootNode.get("Plan Rows").getAsLong();
    }
}
