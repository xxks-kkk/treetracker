package org.zhu45.treetracker.relational.planner.cost;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.zhu45.treetracker.common.SchemaTableName;

import java.util.Set;

import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;

public class TestPostgresCardEstProvider
{
    @DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
    @Test
    public void testPostgresCardEstProvider()
    {
        PostgresCardEstProvider cardEstProvider = new PostgresCardEstProvider();
        SchemaTableName companyType = new SchemaTableName("imdb", "q1a_company_type");
        SchemaTableName movieCompanies = new SchemaTableName("imdb", "q1a_movie_companies");
        SchemaTableName movieInfoIdx = new SchemaTableName("imdb_int", "movie_info_idx");
        cardEstProvider.getSize(Set.of(companyType, movieCompanies, movieInfoIdx), null);
    }
}
