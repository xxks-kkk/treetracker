package org.zhu45.treetracker.relational.planner.testing.postgresplan;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.zhu45.treetracker.common.TreeTrackerException;

import java.util.List;

import static org.zhu45.treetracker.common.StandardErrorCode.INVALID_USAGE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.ACTUAL_ROWS_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.INNER_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.OUTER_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PARENT_RELATIONSHIP_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PLANS_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PLAN_ROWS_VALUE;

@SuperBuilder
public abstract class MockNode
{
    @JsonProperty(index = 2, value = PARENT_RELATIONSHIP_VALUE)
    protected String side;
    @JsonProperty(index = 4, value = PLAN_ROWS_VALUE)
    protected long planRows;
    @JsonProperty(index = 5, value = ACTUAL_ROWS_VALUE)
    protected long actualRows;
    @Getter @Setter @JsonProperty(index = 6, value = PLANS_VALUE)
    public List<MockNode> plans;

    protected MockNode() {}

    public abstract static class MockNodeBuilder<C extends MockNode, B extends MockNodeBuilder<C, B>>
    {
        MockNodeBuilder side(String side)
        {
            if (side.equals(OUTER_VALUE) || side.equals(INNER_VALUE)) {
                this.side = side;
                return this;
            }
            throw new TreeTrackerException(INVALID_USAGE, "Invalid side: " + side);
        }
    }
}
