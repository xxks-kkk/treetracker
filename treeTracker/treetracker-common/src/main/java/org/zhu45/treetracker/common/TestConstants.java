package org.zhu45.treetracker.common;

import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_USER_ERROR;
import static org.zhu45.treetracker.common.TestConstants.Constants.TREETRACKER_AGG_STATS_LOC_VALUE;
import static org.zhu45.treetracker.common.TestConstants.Constants.TREETRACKER_DEBUG_VALUE;

public enum TestConstants
{
    // used to disable certain test cases in github workflow due to Postgres limitation
    GITHUB(Constants.GITHUB_VALUE),
    // whether enable verbose debug logging (maybe it's a good idea to have a separate
    // constant for each module)
    TREETRACKER_DEBUG(TREETRACKER_DEBUG_VALUE),
    // location to save AggregateStatisticsInformation
    TREETRACKER_AGG_STATS_LOC(TREETRACKER_AGG_STATS_LOC_VALUE);

    public static class Constants
    {
        public static final String GITHUB_VALUE = "github";
        public static final String TREETRACKER_DEBUG_VALUE = "treetracker_debug";
        public static final String TREETRACKER_AGG_STATS_LOC_VALUE = "treetracker_agg_stats_loc";
    }

    private final String stringVal;

    TestConstants(String testConstants)
    {
        this.stringVal = testConstants;
    }

    public String getStringVal()
    {
        return this.stringVal;
    }

    public static boolean checkEnvVariableSet(TestConstants constants)
    {
        return System.getenv().containsKey(constants.getStringVal());
    }

    public static String getEnvVariableSet(TestConstants constants)
    {
        if (checkEnvVariableSet(constants)) {
            return System.getenv().get(constants.getStringVal());
        }
        throw new TreeTrackerException(GENERIC_USER_ERROR, String.format("%s is not set.", constants));
    }
}
