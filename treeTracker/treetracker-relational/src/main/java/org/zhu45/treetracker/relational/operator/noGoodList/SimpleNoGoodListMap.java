package org.zhu45.treetracker.relational.operator.noGoodList;

import de.renebergelt.test.Switches;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.OperatorInformation;
import org.zhu45.treetracker.relational.operator.StatisticsInformation;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanStatisticsInformation;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;

import static org.openjdk.jol.info.GraphLayout.parseInstance;

public class SimpleNoGoodListMap
        implements NoGoodListMap
{
    private final NoGoodList noGoodList;
    private StatisticsInformation statisticsInformation;

    private SimpleNoGoodListMap(NoGoodList noGoodList)
    {
        if (Switches.STATS) {
            this.statisticsInformation = new TupleBasedTableScanStatisticsInformation();
        }
        this.noGoodList = noGoodList;
    }

    @Override
    public boolean isGood(Row row)
    {
        if (Switches.STATS) {
            if (noGoodList.contains(row)) {
                statisticsInformation.incrementNumberOfNoGoodTuplesFiltered();
            }
        }
        return !this.noGoodList.contains(row);
    }

    @Override
    public void updateNoGoodListMap(Row row, OperatorInformation info)
    {
        this.noGoodList.add(row);
    }

    @Override
    public String generateNoGoodListMapRepresentation()
    {
        return null;
    }

    @Override
    public int getNoGoodListSize()
    {
        return noGoodList.size();
    }

    @Override
    public int size()
    {
        return noGoodList.size();
    }

    @Override
    public StatisticsInformation getStatisticsInformation()
    {
        return statisticsInformation;
    }

    @Override
    public void updateStatisticsInformation()
    {
        statisticsInformation.setNumberOfNoGoodTuples(noGoodList.size());
    }

    @Override
    public long sizeInBytes()
    {
        return parseInstance(noGoodList).totalSize();
    }

    public static SimpleNoGoodListMap constructSimpleNoGoodListMap(PlanBuildContext planBuildContext)
    {
        return new SimpleNoGoodListMap(planBuildContext.getNoGoodList());
    }
}
