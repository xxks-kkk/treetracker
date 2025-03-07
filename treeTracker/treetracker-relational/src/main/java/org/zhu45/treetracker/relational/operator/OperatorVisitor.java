package org.zhu45.treetracker.relational.operator;

public abstract class OperatorVisitor<C>
{
    void process(Operator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedTreeTrackerOneBetaHashTableOperator(TupleBaseTreeTrackerOneBetaHashTableOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedTreeTrackerOneBetaHashTableIntOperator(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedTableScanOperator(TupleBasedTableScanOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedLIPTableScanOperator(TupleBasedLIPTableScanOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedNestedLoopJoinOperator(TupleBasedNestedLoopJoinOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedHashJoinOperator(TupleBasedHashJoinOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedLIPHashJoinOperator(TupleBasedLIPHashJoinOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitFullReducerOperator(FullReducerOperator operator, C context)
    {
        operator.accept(this, context);
    }

    void visitTupleBasedLeftSemiBloomJoinOperator(TupleBasedLeftSemiBloomJoinOperator operator, C context)
    {
        operator.accept(this, context);
    }
}
