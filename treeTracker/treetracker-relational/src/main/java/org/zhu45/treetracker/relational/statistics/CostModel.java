package org.zhu45.treetracker.relational.statistics;

import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.math.BigDecimal;

public interface CostModel
{
    BigDecimal getCost(JoinOperator joinAlgorithm);
}
