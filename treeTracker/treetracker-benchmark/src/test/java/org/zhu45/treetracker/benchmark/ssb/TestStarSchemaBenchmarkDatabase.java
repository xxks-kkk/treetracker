package org.zhu45.treetracker.benchmark.ssb;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;

import java.util.Map;

import static org.zhu45.treetracker.common.TestConstants.GITHUB;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestStarSchemaBenchmarkDatabase
{
    private static Logger log = LogManager.getLogger(TestStarSchemaBenchmarkDatabase.class);
    private ConnectionLeakUtil connectionLeakUtil;

    @BeforeAll
    public void setUp()
    {
        Configurator.setAllLevels(TestStarSchemaBenchmarkDatabase.class.getName(), Level.DEBUG);
        Configurator.setAllLevels(StarSchemaBenchmarkDatabase.class.getName(), Level.INFO);
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    /**
     * Alternatively, use
     * $ time psql -p5432 -d postgres -f treetracker-benchmark/src/main/resources/ssb.sql
     * to ingest benchmark database
     */
    @Disabled("Benchmark Data Ingestion Finished. Disable to avoid database override")
    @Test
    public void testStarSchemaBenchmarkDatabase()
    {
        Map<String, String> env = System.getenv();
        Assumptions.assumingThat(!env.containsKey(GITHUB.getStringVal()), () -> {
            log.info("assumption satisfied and test StarSchemaBenchmarkDatabase()");
            StarSchemaBenchmarkDatabase database = new StarSchemaBenchmarkDatabase();
        });
    }

    @AfterAll
    public void assertNoLeaks()
    {
        connectionLeakUtil.assertNoLeaks();
    }
}
