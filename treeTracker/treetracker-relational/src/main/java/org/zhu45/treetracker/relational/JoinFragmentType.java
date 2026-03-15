package org.zhu45.treetracker.relational;

import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.List;

public interface JoinFragmentType
{
    void eval();

    void open();

    long evalWithResultSize();

    void evalWithoutOpen();

    List<Operator> getOperators();

    Operator getRootOperator();

    void cleanUp();

    int getNumRelations();

    JoinOperator getAlgorithm();

    String getQueryName();

    Plan getPlan();

    void populateDomain();
}
