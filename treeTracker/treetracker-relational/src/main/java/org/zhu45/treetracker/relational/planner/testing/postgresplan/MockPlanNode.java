package org.zhu45.treetracker.relational.planner.testing.postgresplan;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.ACTUAL_ROWS_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PLAN_ROWS_VALUE;

@Getter
@JsonIgnoreProperties(value = {PLAN_ROWS_VALUE, ACTUAL_ROWS_VALUE})
public class MockPlanNode
        extends MockNode
{
    @JsonProperty("Plan")
    MockNode planNode;

    public MockPlanNode(MockNode planNode)
    {
        super();
        this.planNode = planNode;
    }
}
