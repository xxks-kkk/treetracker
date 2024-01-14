package org.zhu45.treetracker.common.row;

import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.Value;
import org.zhu45.treetracker.common.type.Type;

import java.io.Serializable;
import java.util.List;

public interface Row
        extends Iterable<RelationalValue>, Value, Serializable
{
    List<String> getAttributes();
    List<Type> getTypes();
    List<RelationalValue> getVals();
    int size();
    void setIsGood(boolean b);
    boolean getIsGood();
    RowType getRowType();
}
