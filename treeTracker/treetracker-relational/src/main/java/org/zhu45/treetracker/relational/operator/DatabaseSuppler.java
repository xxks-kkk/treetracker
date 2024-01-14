package org.zhu45.treetracker.relational.operator;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.logging.LoggerProvider;

import java.sql.SQLException;
import java.util.Optional;
import java.util.Random;

import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.naturalJoinJdbcClientSupplier;

/**
 * Provide a database to be used throughout different tests.
 */
public class DatabaseSuppler
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(DatabaseSuppler.class);
    private static final int NUM_RELATIONS = 5;

    @SuppressWarnings("checkstyle:StaticVariableName")
    public static Supplier<TestingMultiwayJoinDatabaseComplex> TestingMultiwayJoinDatabaseComplexSupplier = Suppliers.memoize(() -> {
        Random rand = new Random();
        Optional<Long> seed = Optional.of(rand.nextLong());
        log.info("seed: " + seed);

        try {
            return new TestingMultiwayJoinDatabaseComplex(
                    naturalJoinJdbcClientSupplier.get(),
                    NUM_RELATIONS,
                    seed);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, "TestingMultiwayJoinDatabaseComplex creation failed");
    });

    private DatabaseSuppler()
    {
    }
}
