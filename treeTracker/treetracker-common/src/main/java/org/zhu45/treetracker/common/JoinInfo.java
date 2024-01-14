package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.type.Type;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JoinInfo
{
    private ObjectRow rone;
    private ObjectRow rtwo;
    // record which columns (attributes) of S we know that are join attributes
    private Set<Integer> processedRelationSColumn;
    // <index of column in R, index of column in S> are potentially joinable (their values may be different)
    private Map<Integer, Integer> joinIdx;
    // <index of column in S, index of column in R> are potentially joinable
    private Map<Integer, Integer> inverseJoinIdx;
    // record which columns (attributes) of R we know that are join attributes
    private Set<Integer> processedRelationRColumn;
    // join result relation attributes
    private List<String> resultTupleAttributes;
    // join result relation types
    private List<Type> types;
    // indicate whether the given two rows are joinable
    private boolean isJoinable;

    public JoinInfo(ObjectRow rone, ObjectRow rtwo)
    {
        this.rone = rone;
        this.rtwo = rtwo;
        processedRelationSColumn = new HashSet<>();
        resultTupleAttributes = new ArrayList<>();
        types = new ArrayList<>();
        List<String> roneAttributes = rone.getAttributes();
        List<String> rtwoAttributes = rtwo.getAttributes();
        List<Type> roneTypes = rone.getTypes();
        List<Type> rtwoTypes = rtwo.getTypes();
        joinIdx = new HashMap<>();
        for (int i = 0; i < rone.size(); ++i) {
            for (int j = 0; j < rtwo.size(); ++j) {
                if (roneAttributes.get(i).equals(rtwoAttributes.get(j)) && roneTypes.get(i).equals(rtwoTypes.get(j))) {
                    joinIdx.put(i, j);
                }
            }
        }
        for (int i = 0; i < rone.size(); ++i) {
            if (joinIdx.containsKey(i)) {
                if (!rone.entryEqual(rtwo, i, joinIdx.get(i))) {
                    // Because we assume the natural join query, if any of the potential joinable attribute's
                    // value cannot be joined, we should remove all the potential join attributes.
                    // For example, potential joinable attributes: [age, name]. rone: [49, Amelia], rtwo: [76, Amelia]
                    // Since 49 != 76, this two rows cannot be joined by natural join definition.
                    joinIdx.clear();
                }
                else {
                    processedRelationSColumn.add(joinIdx.get(i));
                }
            }
            resultTupleAttributes.add(roneAttributes.get(i));
            types.add(roneTypes.get(i));
        }
        if (!joinIdx.isEmpty()) {
            isJoinable = true;
            inverseJoinIdx = new HashMap<>();
            processedRelationRColumn = new HashSet<>();
            for (Integer i : joinIdx.keySet()) {
                inverseJoinIdx.put(joinIdx.get(i), i);
                processedRelationRColumn.add(i);
            }
        }
        for (int i = 0; i < rtwo.size(); ++i) {
            if (!processedRelationSColumn.contains(i)) {
                resultTupleAttributes.add(rtwoAttributes.get(i));
                types.add(rtwoTypes.get(i));
            }
        }
    }

    public boolean isJoinable()
    {
        return isJoinable;
    }

    public List<Type> getTypes()
    {
        return types;
    }
}
