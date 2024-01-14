package org.zhu45.treetracker.relational.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.relational.planner.Plan;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_USER_ERROR;

/**
 * Dump stats related to JSON
 */
public class StatisticsInformationToJson
{
    private Operator rootOperator;
    private AggregateStatisticsInformation aggregateStatisticsInformation;
    private ObjectMapper objectMapper;
    private ObjectNode rootNode;

    private StatisticsInformationToJson(Operator rootOperator,
                                        AggregateStatisticsInformation aggregateStatisticsInformation)
    {
        this.rootOperator = rootOperator;
        this.aggregateStatisticsInformation = aggregateStatisticsInformation;
        this.objectMapper = new ObjectMapper();
        objectMapper.setAnnotationIntrospector(new IgnoreInheritedIntrospector());
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.rootNode = JsonNodeFactory.instance.objectNode();
    }

    public static class IgnoreInheritedIntrospector
            extends JacksonAnnotationIntrospector
    {
        @Override
        public boolean hasIgnoreMarker(final AnnotatedMember m)
        {
            return m.getDeclaringClass() == AbstractStatisticsInformation.class || super.hasIgnoreMarker(m);
        }
    }

    public static void generateStatisticsInformationJson(Operator rootOperator,
                                                         AggregateStatisticsInformation aggregateStatisticsInformation,
                                                         JoinOperator algorithm,
                                                         String queryName,
                                                         String resultStoredPath)
    {
        Path csvPath = Paths.get(resultStoredPath, algorithm + "_" + queryName + ".json");
        StatisticsInformationToJson statisticsInformationToJson = new StatisticsInformationToJson(rootOperator, aggregateStatisticsInformation);
        try {
            ObjectNode root = statisticsInformationToJson.objectMapper.valueToTree(statisticsInformationToJson.rootNode);
            statisticsInformationToJson.addObjectToNode(root, "Aggregation Stats", aggregateStatisticsInformation);
            if (rootOperator != null) {
                statisticsInformationToJson.printHelper(rootOperator, root);
            }
            statisticsInformationToJson.objectMapper.writeValue(csvPath.toFile(), root);
        }
        catch (IOException e) {
            throw new TreeTrackerException(GENERIC_USER_ERROR, "Exception happened when write statistics to JSON\n" + e);
        }
    }

    private void printHelper(Operator rootOperator, ObjectNode root)
    {
        OperatorVisitor<ObjectNode> printer = new OperatorVisitor<ObjectNode>()
        {
            @Override
            public void visitTupleBasedTreeTrackerOneBetaHashTableOperator(TupleBaseTreeTrackerOneBetaHashTableOperator operator, ObjectNode root)
            {
                ObjectNode newNode = addObjectToNode(root, operator.getTraceOperatorName(), operator.getStatisticsInformation());
                process(operator.r1Operator, newNode);
                process(operator.r2Operator, newNode);
            }

            @Override
            public void visitTupleBasedTableScanOperator(TupleBasedTableScanOperator operator, ObjectNode root)
            {
                addObjectToNode(root, operator.getTraceOperatorName() + operator.getOperatorID(), operator.getStatisticsInformation());
            }

            @Override
            public void visitTupleBasedNestedLoopJoinOperator(TupleBasedNestedLoopJoinOperator operator, ObjectNode root)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visitTupleBasedLeftSemiBloomJoinOperator(TupleBasedLeftSemiBloomJoinOperator operator, ObjectNode root)
            {
                ObjectNode newNode = addObjectToNode(root, operator.getTraceOperatorName(), operator.getStatisticsInformation());
                process(operator.r1Operator, newNode);
                process(operator.r2Operator, newNode);
            }

            @Override
            public void visitTupleBasedHashJoinOperator(TupleBasedHashJoinOperator operator, ObjectNode root)
            {
                ObjectNode newNode = addObjectToNode(root, operator.getTraceOperatorName(), operator.getStatisticsInformation());
                process(operator.r1Operator, newNode);
                process(operator.r2Operator, newNode);
            }

            @Override
            public void visitTupleBasedLIPTableScanOperator(TupleBasedLIPTableScanOperator operator, ObjectNode root)
            {
                addObjectToNode(root, operator.getTraceOperatorName() + operator.getOperatorID(), operator.getStatisticsInformation());
            }

            @Override
            public void visitTupleBasedLIPHashJoinOperator(TupleBasedLIPHashJoinOperator operator, ObjectNode root)
            {
                ObjectNode newNode = addObjectToNode(root, operator.getTraceOperatorName(), operator.getStatisticsInformation());
                process(operator.r1Operator, newNode);
                process(operator.r2Operator, newNode);
            }

            @Override
            public void visitFullReducerOperator(FullReducerOperator operator, ObjectNode root)
            {
                ObjectNode newNode = addObjectToNode(root, operator.getTraceOperatorName(), operator.getStatisticsInformation());
                for (Plan plan : operator.getBottomUpSemijoins()) {
                    process(plan.getRoot().getOperator(), newNode);
                }
                for (Plan plan : operator.getTopDownSemijoins()) {
                    process(plan.getRoot().getOperator(), newNode);
                }
                process(operator.getSinkOperator(), root);
            }
        };
        printer.process(rootOperator, root);
    }

    private ObjectNode addObjectToNode(ObjectNode root, String propertyName, Object fromValue)
    {
        ObjectNode newNode = root.putObject(propertyName);
        ObjectNode fromValueNode = objectMapper.valueToTree(fromValue);
        newNode.setAll(fromValueNode);
        return newNode;
    }
}
