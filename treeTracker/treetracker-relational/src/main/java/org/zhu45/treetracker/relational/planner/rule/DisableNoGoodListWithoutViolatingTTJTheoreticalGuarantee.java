package org.zhu45.treetracker.relational.planner.rule;

import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcColumnHandle;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TreeTrackerTableScanV2Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.catalog.TableCatalog;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createTableScanOperator;

/**
 * In this rule, we disable the no-good list map
 * if the columns associated with join idxes of R_k are all
 * unique values because any no-good jav stored in NoGoodListMap
 * will not be able to filter out any tuple.
 * <p>
 * For example, in Q1aOptJoinTreeOptOrdering, R_k is title
 * and its only child is movie_companies and the column of their join idx
 * is movie_id. Movie_id is the primary key of title. Thus, any no-good jav
 * built from movie_id and accumulated in NoGoodListMap will not filter
 * out any tuples.
 * <p>
 * This rule can extend to cover join idx with more than 1 attribute. For example,
 * in Q12bOptJoinTreeOptOrdering, R_k is movie_info and one of its child
 * is movie_info_idx. The join idx is on (movie_id, info_type_id). If column value
 * combinations on the join idx from R_k are all unique, we should not have
 * no-good jav built for this join idx.
 * <p>
 * This naturally leads to three optimizations (Roadmap):
 * 1. We focus on simple case like Q1a and disable no-good list completely when
 * the aforementioned condition is satisfied
 * 2. The above optimization is a special case of deciding how many NoGoodList
 * exists in NoGoodListMap, which, in this case, is 0. We can decide how many
 * NoGoodList exists in NoGoodListMap due to the column uniqueness. Then, NoGoodList
 * Map construction takes the decision to construct NoGoodListMap accordingly. Note,
 * instead of constructing lazily (in 244-lazily-build-nogoodlistmap-fix2 branch),
 * we can construct all at once but potentially with fewer entries (constructing lazily
 * tries to solve this problem as well without using Rule because entry is added on PassContext
 * but such entry may be corresponding unique values, which are useless to store them
 * in the noGoodListMap. Thus, constructing lazily is orthogonal to this optimization.
 * 3. Based on uniqueness, we can enable order by optimization (#247) and use order by
 * on the column with the least unique fraction ratio (thus, potentially, number of
 * no-good jav the most if we use set).
 */
public class DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee
        extends BaseRule
{
    @Override
    public Plan applyToLogicalPlan(Plan plan, PlanBuildContext context)
    {
        return plan;
    }

    @Override
    public Plan applyToPhysicalPlan(PlanNode node, RuleContext context)
    {
        ruleStatistics = RuleStatistics.builder(this.getClass()).build();
        TableNode rootTableNode = (TableNode) node.getSources().get(0);
        if (canDisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee(context, rootTableNode)) {
            // create new normal table scan operator and replace the R_k node table scan operator
            try {
                TupleBasedTableScanOperator tableScanOperator = createTableScanOperator(rootTableNode,
                        TupleBasedTableScanOperator.class.getConstructor(),
                        context.getPlanBuildContext(),
                        new HashMap<>());
                Operator joinOperator = node.getOperator();
                List<Operator> childOperators = node.getSources().stream().map(PlanNode::getOperator).collect(Collectors.toList());
                childOperators.set(0, tableScanOperator);
                joinOperator.setChildren(childOperators);
                tableScanOperator.setOperatorID(node.getId());
                tableScanOperator.setOperatorTraceDepth(rootTableNode.getOperator().getOperatorTraceDepth());
                rootTableNode.setOperator(tableScanOperator);
            }
            catch (NoSuchMethodException e) {
                throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
            }
        }
        return null;
    }

    private boolean canDisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee(RuleContext context, TableNode tableNode)
    {
        int numberOfEntriesHavingAllUniqueValues = 0;
        PlanBuildContext planBuildContext = context.getPlanBuildContext();
        checkState(planBuildContext != null);
        Map<Integer, List<Integer>> nodeId2JoinIdx = planBuildContext.getNodeId2FactTableJoinAttributeIdx();
        TableCatalog tableCatalog = planBuildContext.getCatalogGroup().getTableCatalog(tableNode.getSchemaTableName());
        float[] fractionOfUniquesInEachColumn = tableCatalog.getFractionOfUniquesInEachColumn();
        checkState(nodeId2JoinIdx != null, "nodeId2JoinIdx cannot be null to apply " + this.getClass().getCanonicalName());
        for (List<Integer> factTableJoinAttributeIdx : nodeId2JoinIdx.values()) {
            if (factTableJoinAttributeIdx.size() == 1) {
                if (Float.compare(fractionOfUniquesInEachColumn[factTableJoinAttributeIdx.get(0)], 1.0f) == 0) {
                    numberOfEntriesHavingAllUniqueValues += 1;
                }
            }
            else {
                JdbcClient jdbcClient = planBuildContext.getJdbcClient();
                List<JdbcColumnHandle> targetColumnHandles = new ArrayList<>();
                for (Integer joinIdx : factTableJoinAttributeIdx) {
                    targetColumnHandles.add(tableCatalog.getColumnHandles().get(joinIdx));
                }
                float fractionOfUniqueValues = jdbcClient.fractionOfUniqueValuesInColumn((JdbcTableHandle) tableCatalog.getTableHandle(),
                        targetColumnHandles);
                if (Float.compare(fractionOfUniqueValues, 1.0f) == 0) {
                    numberOfEntriesHavingAllUniqueValues += 1;
                }
            }
        }
        return numberOfEntriesHavingAllUniqueValues == nodeId2JoinIdx.size();
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        if (node.getNodeType() != OptType.join) {
            return false;
        }
        PlanNode leftChildNode = node.getSources().get(0);
        if (leftChildNode.getNodeType() != OptType.table) {
            return false;
        }
        TableNode leftChildTableNode = (TableNode) leftChildNode;
        if (leftChildTableNode.getMultiwayJoinNode() == null) {
            return false;
        }
        return leftChildTableNode.getMultiwayJoinNode().getNodeType() == NodeType.Root &&
                (leftChildTableNode.getOperator().getClass() == TupleBasedHighPerfTableScanOperator.class ||
                        leftChildTableNode.getOperator().getClass() == TreeTrackerTableScanV2Operator.class);
    }

    @Override
    public RuleType getRuleType()
    {
        return RuleType.IN_PLACE;
    }
}
