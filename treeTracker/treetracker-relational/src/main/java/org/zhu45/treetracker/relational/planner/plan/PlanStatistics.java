package org.zhu45.treetracker.relational.planner.plan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.RuleStatistics;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_USER_ERROR;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class PlanStatistics
{
    // the join orderings explored (from FindTheBestJoinOrdering rule)
    private HashMap<JoinOrdering, Float> searchedPlan;
    // the optimal join ordering
    private JoinOrdering optimalJoinOrdering;
    // join trees explored (from FindOptimalJoinTree rule)
    private HashMap<MultiwayJoinOrderedGraph, Double> searchedJoinTrees;
    // the optimal join tree found (from FindOptimalJoinTree rule)
    private MultiwayJoinOrderedGraph optimalJoinTree;
    // measures the quality of the selected plan, i.e., cost, which can be determined by arbitrary method
    private double cost;
    // NoGoodListMap implementation used
    private String noGoodListMapClazzName;
    // List of rules applied
    private final List<String> rulesApplied = new ArrayList<>();
    @JsonIgnore
    private FindTheBestJoinOrderingWithDP.DPTable dpTable;
    // FIXME: In the future, we should only keep this field: above fields should
    // not mangle inside PlanStatistics; instead, they belong to each RuleStatistics
    private final List<RuleStatistics> ruleStatisticsList = new ArrayList<>();

    public PlanStatistics()
    {
    }

    public void mergeRuleStatistics(RuleStatistics ruleStatistics)
    {
        searchedPlan = ruleStatistics.getSearchedPlan();
        optimalJoinOrdering = ruleStatistics.getOptimalJoinOrdering();
        searchedJoinTrees = ruleStatistics.getSearchedJoinTrees();
        optimalJoinTree = ruleStatistics.getOptimalJoinTree();
        cost = ruleStatistics.getCost();
        rulesApplied.add(ruleStatistics.getRuleName());
        dpTable = ruleStatistics.getDpTable();
        ruleStatisticsList.add(ruleStatistics);
    }

    public HashMap<JoinOrdering, Float> getSearchedPlan()
    {
        return searchedPlan;
    }

    public JoinOrdering getOptimalJoinOrdering()
    {
        return optimalJoinOrdering;
    }

    public MultiwayJoinOrderedGraph getOptimalJoinTree()
    {
        return optimalJoinTree;
    }

    public HashMap<MultiwayJoinOrderedGraph, Double> getSearchedJoinTrees()
    {
        return searchedJoinTrees;
    }

    public double getCost()
    {
        return cost;
    }

    public void setNoGoodListMapClazz(String noGoodListMapClazzName)
    {
        this.noGoodListMapClazzName = noGoodListMapClazzName;
    }

    public String getNoGoodListMapClazzName()
    {
        return noGoodListMapClazzName;
    }

    public List<String> getRulesApplied()
    {
        return rulesApplied;
    }

    public FindTheBestJoinOrderingWithDP.DPTable getDpTable()
    {
        return dpTable;
    }

    public List<RuleStatistics> getRuleStatisticsList()
    {
        return ruleStatisticsList;
    }

    public Path save(String queryName, JoinOperator algorithm, String resultStoredPath)
    {
        PlanStatisticsToJson planStatisticsToJson = new PlanStatisticsToJson();
        return planStatisticsToJson.save(this, queryName, algorithm, resultStoredPath);
    }

    private static final class PairSerializer
            extends JsonSerializer<Pair>
    {
        @Override
        public void serialize(Pair value, JsonGenerator gen, SerializerProvider serializers) throws IOException
        {
            gen.writeStartObject();
            gen.writeObjectField("left", value.getLeft());
            gen.writeObjectField("right", value.getRight());
            gen.writeEndObject();
        }
    }

    @JsonIgnoreType
    private static class PlanStatisticsToJson
    {
        private final ObjectMapper objectMapper;

        public PlanStatisticsToJson()
        {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new SimpleModule().addSerializer(Pair.class, new PairSerializer()));
        }

        public Path save(PlanStatistics planStatistics, String queryName, JoinOperator algorithm, String resultStoredPath)
        {
            Path jsonPath = Paths.get(resultStoredPath, algorithm + "_" + queryName + "_" + "planStatistics.json");
            try {
                objectMapper
                        .writerWithDefaultPrettyPrinter()
                        .writeValue(jsonPath.toFile(), planStatistics);
            }
            catch (IOException e) {
                throw new TreeTrackerException(GENERIC_USER_ERROR, "Exception happened when write statistics to JSON\n" + e);
            }
            return jsonPath;
        }
    }
}
