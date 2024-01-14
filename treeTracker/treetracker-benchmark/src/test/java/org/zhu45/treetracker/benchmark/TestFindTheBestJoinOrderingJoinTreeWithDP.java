package org.zhu45.treetracker.benchmark;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.benchmark.job.JOBQueriesAutoGen;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstProvider;
import org.zhu45.treetracker.relational.planner.cost.TTJCardEstProvider;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingJoinTreeWithDP;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProviderConfiguration.defaultConfiguration;
import static org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP.DPTableEntryId.getDPTableEntryId;

/**
 * TODO: probably we can add one more test case on query has join tree with branches. We can find
 * such query once we regenerate all the *OptJoinTreeOptOrdering
 */
@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
public class TestFindTheBestJoinOrderingJoinTreeWithDP
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TestFindTheBestJoinOrderingJoinTreeWithDP.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @Test
    public void test()
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP,
                JOBQueriesAutoGen.Query1bOptJoinTreeOptOrdering,
                List.of(new FindTheBestJoinOrderingJoinTreeWithDP(new TTJCardEstProvider(new JoinTreeCostEstProvider(defaultConfiguration)))),
                duckDBJdbcClientSupplier.get());
        FindTheBestJoinOrderingWithDP.DPTable dpTable = query.getPlan().getPlanStatistics().getDpTable();

        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(dpTable);
        }

        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q1b, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q1b);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q1b, null);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q1b, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q1b, null);

        // Test on cell {imdb_int.movie_info_idx,imdb.q1b_title}
        {
            FindTheBestJoinOrderingWithDP.DPTableEntryId dpTableEntryId = getDPTableEntryId(
                    Set.of(movieInfoIdxNode.getSchemaTableName(), titleNode.getSchemaTableName()));
            FindTheBestJoinOrderingWithDP.DPTableEntry dpTableEntry = dpTable.getDPEntry(dpTableEntryId);
            List<String> sqls = dpTableEntry.getCardEstReturn().getCostReturn().getSqls();
            // 3 because
            // - 1 is for estimating the number of dangling tuples
            // - 1 is for estimating the size of R_k^*
            // - 1 is for estimating the |movie_info_idx \join title|
            // - 1 is for estimating the size of inner relation that is in clean state
            assertEquals(4, sqls.size());
            assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT distinct(movie_id) FROM \"postgres\".\"imdb\".\"q1b_title\" WHERE (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"imdb_int\".\"movie_info_idx\")",
                    sqls.get(0));
        }

        // Test on cell {imdb_int.movie_info_idx,imdb.q1b_movie_companies,imdb.q1b_company_type}
        {
            FindTheBestJoinOrderingWithDP.DPTableEntryId dpTableEntryId = getDPTableEntryId(
                    Set.of(movieInfoIdxNode.getSchemaTableName(),
                            movieCompaniesNode.getSchemaTableName(),
                            companyTypeNode.getSchemaTableName()));
            FindTheBestJoinOrderingWithDP.DPTableEntry dpTableEntry = dpTable.getDPEntry(dpTableEntryId);
            List<String> sqls = dpTableEntry.getCardEstReturn().getCostReturn().getSqls();
            // 5 because (join tree: ct - mc - mii)
            // - 1 is for estimating the number of dangling tuples from ct
            // - 1 is for estimating the size of R_k^*
            // - 1 is for estimating the number of dangling tuples from mc
            // - 1 is for |ct \join mc|
            // - 1 is for |ct \join mc \join mii|
            // - 1 is for estimating the size of mc in clean state
            // - 1 is for estimating the size of mii in clean state
            assertEquals(7, sqls.size());
            assertTrue(sqls.contains("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT distinct(company_type_id) FROM \"postgres\".\"imdb\".\"q1b_company_type\" WHERE (company_type_id) NOT IN (SELECT company_type_id FROM \"postgres\".\"imdb\".\"q1b_movie_companies\" EXCEPT ALL SELECT company_type_id FROM \"postgres\".\"imdb\".\"q1b_movie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"imdb\".\"q1b_company_type\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"imdb_int\".\"movie_info_idx\"))"));
        }
        // Test {imdb.q1b_info_type,imdb_int.movie_info_idx,imdb.q1b_movie_companies,imdb.q1b_title}
        {
            FindTheBestJoinOrderingWithDP.DPTableEntryId dpTableEntryId = getDPTableEntryId(
                    Set.of(infoTypeNode.getSchemaTableName(),
                            movieInfoIdxNode.getSchemaTableName(),
                            movieCompaniesNode.getSchemaTableName(),
                            titleNode.getSchemaTableName()));
            FindTheBestJoinOrderingWithDP.DPTableEntry dpTableEntry = dpTable.getDPEntry(dpTableEntryId);
            List<String> sqls = dpTableEntry.getCardEstReturn().getCostReturn().getSqls();
            // 10 because 3 sqls on estimating the number of dangling tuples, 1 sql on estimating the size of R_k^*, 3 sql on size
            // of intermediate results that are part of join result, and 3 sqls on size of inner relations that are in clean state
            assertEquals(10, sqls.size());
            assertTrue(sqls.contains("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT distinct(info_type_id) FROM \"postgres\".\"imdb\".\"q1b_info_type\" WHERE (info_type_id) NOT IN (SELECT info_type_id FROM \"postgres\".\"imdb_int\".\"movie_info_idx\" EXCEPT ALL SELECT info_type_id FROM \"postgres\".\"imdb_int\".\"movie_info_idx\" WHERE (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"imdb\".\"q1b_info_type\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"imdb\".\"q1b_title\") EXCEPT ALL SELECT info_type_id FROM \"postgres\".\"imdb_int\".\"movie_info_idx\" WHERE (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"imdb\".\"q1b_info_type\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"imdb\".\"q1b_movie_companies\"))"));
            assertTrue(sqls.contains("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT movie_id,info_type_id FROM \"postgres\".\"imdb_int\".\"movie_info_idx\" WHERE (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"imdb\".\"q1b_info_type\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"imdb\".\"q1b_title\")"));
        }
    }
}
