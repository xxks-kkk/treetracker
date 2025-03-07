package org.zhu45.treetracker.relational.planner.testing.postgresplan;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.experimental.SuperBuilder;

import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.GATHER_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.NODE_TYPE_VALUE;

@SuperBuilder
public class MockGatherNode
        extends MockNode
{
    @Builder.Default
    @JsonProperty(index = 1, value = NODE_TYPE_VALUE)
    public String nodeType = GATHER_VALUE;
}
