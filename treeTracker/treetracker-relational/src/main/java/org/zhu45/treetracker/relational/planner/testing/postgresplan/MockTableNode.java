package org.zhu45.treetracker.relational.planner.testing.postgresplan;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.experimental.SuperBuilder;

import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.NODE_TYPE_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.RELATION_NAME_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.SEQ_SCAN_VALUE;

@SuperBuilder
public class MockTableNode
        extends MockNode
{
    @Builder.Default
    @JsonProperty(index = 1, value = NODE_TYPE_VALUE)
    public String nodeType = SEQ_SCAN_VALUE;
    @JsonProperty(index = 3, value = RELATION_NAME_VALUE)
    public String relationName;
}
