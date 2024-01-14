package org.zhu45.treetracker.relational.operator;

public class AggregateStatisticsInformationFactory
{
    Operator rootOperator;
    AggregateStatisticsInformation aggregateStatisticsInformation;

    public AggregateStatisticsInformationFactory(AggregateStatisticsInformationContext context)
    {
        this.rootOperator = context.getRootOperator();
        switch (context.getAlgorithm()) {
            case TTJHP:
            case TTJV1:
            case TTJV2:
            case TTJHP_NO_NG:
            case TTJHP_BF:
            case TTJHP_BG:
                aggregateStatisticsInformation = new TTJAggregateStatisticsInformation();
                break;
            case Yannakakis:
            case YannakakisB:
            case PTO:
            case YannakakisVanilla:
                aggregateStatisticsInformation = new YannakakisAggregateStatisticsInformation();
                break;
            case LIP:
                aggregateStatisticsInformation = new LIPAggregateStatisticsInformation();
                break;
            default:
                aggregateStatisticsInformation = new AggregateStatisticsInformation();
        }
        aggregateStatisticsInformation.setResutSetSize(context.getResultSetSize());
        aggregateStatisticsInformation.setAlgorithm(context.getAlgorithm().name());
        aggregateStatisticsInformation.setQueryName(context.getQueryName());
        aggregateStatisticsInformation.setNumRelations(context.getNumRelations());
        aggregateStatisticsInformation.setRuntime(context.getRuntime());
        aggregateStatisticsInformation.setEvaluationMemoryCostInBytes(context.getEvaluationMemoryCostInBytes());
        if (context.getCostModel() != null) {
            aggregateStatisticsInformation.setCostModel4WeakCost(context.getCostModel().getCost(context.getAlgorithm()));
        }
    }

    public AggregateStatisticsInformation get()
    {
        aggregateStatisticsInformation.printHelper(rootOperator);
        return aggregateStatisticsInformation;
    }
}
