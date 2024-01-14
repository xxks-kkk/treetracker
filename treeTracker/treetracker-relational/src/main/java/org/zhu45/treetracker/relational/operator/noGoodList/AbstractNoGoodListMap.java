package org.zhu45.treetracker.relational.operator.noGoodList;

import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.operator.StatisticsInformation;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;

public abstract class AbstractNoGoodListMap
        implements NoGoodListMap
{
    // if the size of noGoodList is larger than this number, we omit to print it when construct
    // string representation of noGoodListMap
    protected static final int maxPrintNoGoodListSize = 10;

    protected static Logger traceLogger;

    protected long noGoodListProbingTime;
    protected long noGoodListProbingTimeMarker;
    protected long noGoodListConstructTime;
    protected long noGoodListConstructTimeMarker;

    protected int traceDepth;
    protected StatisticsInformation statisticsInformation;
    protected PlanBuildContext planBuildContext;
    // The schemaTableName that this NoGoodListMap is associated with, i.e., R_k
    protected SchemaTableName associateSchemaTableName;

    // whether noGoodListMap has added value (an optimization to skip no-good list check if there hasn't been
    // any value add to it.
    protected boolean noGoodListMapAddedValue;

    public String formatTraceMessage(String message)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("|".repeat(Math.max(0, traceDepth)))
                .append(message);
        return stringBuilder.toString();
    }

    @Override
    public StatisticsInformation getStatisticsInformation()
    {
        return statisticsInformation;
    }

    @Override
    public void updateStatisticsInformation()
    {
        statisticsInformation.setNoGoodListConstructTime(noGoodListConstructTime);
        statisticsInformation.setNoGoodListProbingTime(noGoodListProbingTime);
        statisticsInformation.setNumberOfNoGoodTuples(getNoGoodListSize());
        statisticsInformation.setNoGoodListSizeInBytes(sizeInBytes());
        statisticsInformation.setNoGoodListMapRepresentation(generateNoGoodListMapRepresentation());
        statisticsInformation.setNoGoodListMapKeyNum(size());
    }
}
