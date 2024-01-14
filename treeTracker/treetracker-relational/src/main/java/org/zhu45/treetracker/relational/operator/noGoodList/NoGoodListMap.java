package org.zhu45.treetracker.relational.operator.noGoodList;

import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.OperatorInformation;
import org.zhu45.treetracker.relational.operator.StatisticsInformation;

public interface NoGoodListMap
{
    boolean isGood(Row row);

    void updateNoGoodListMap(Row row, OperatorInformation info);

    String generateNoGoodListMapRepresentation();

    int getNoGoodListSize();

    int size();

    StatisticsInformation getStatisticsInformation();

    void updateStatisticsInformation();

    long sizeInBytes();
}
