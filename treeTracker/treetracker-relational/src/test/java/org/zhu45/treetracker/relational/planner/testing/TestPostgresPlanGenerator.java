package org.zhu45.treetracker.relational.planner.testing;

import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanBuilder;
import org.zhu45.treetracker.relational.planner.PlanNodeIdAllocator;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.relational.planner.testing.postgresplan.PostgresPlanGenerator.generatePostgresPlanJson;

public class TestPostgresPlanGenerator
{
    private void checkPlanCanBeBuilt(String postgresPlan)
    {
        PlanBuildContext context = PlanBuildContext.builder()
                                                   .setPlanNodeIdAllocator(new PlanNodeIdAllocator())
                                                   .setRules(List.of())
                                                   .planBuildOption(PlanBuildContext.PlanBuildOption.POSTGRES)
                                                   .postgresPlan(postgresPlan)
                                                   .schema("public")
                                                   .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        planBuilder.build();
    }

    @Test
    public void test()
    {
        String specification = "JOIN,Outer,1,1\n" +
                               "|JOIN,Outer,10,100\n" +
                               "||TAB,R,Outer,10,10\n" +
                               "||TAB,S,Inner,6,10\n" +
                               "|JOIN,Inner,11,5\n" +
                               "||TAB,U,Inner,10,10\n" +
                               "||TAB,V,Outer,5,5";
        String postgresPlan = generatePostgresPlanJson(specification);
        String expectedPostgresPlan = "[{\"Plan\":{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":1,\"Actual Rows\":1,\"Plans\":[{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":10,\"Actual Rows\":100,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"R\",\"Plan Rows\":10,\"Actual Rows\":10},{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Inner\",\"Relation Name\":\"S\",\"Plan Rows\":6,\"Actual Rows\":10}]},{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Inner\",\"Plan Rows\":11,\"Actual Rows\":5,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Inner\",\"Relation Name\":\"U\",\"Plan Rows\":10,\"Actual Rows\":10},{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"V\",\"Plan Rows\":5,\"Actual Rows\":5}]}]}}]";
        assertEquals(expectedPostgresPlan, postgresPlan);
        checkPlanCanBeBuilt(postgresPlan);
    }

    @Test
    public void test2()
    {
        String specification = "AGG,Outer\n" +
                               "|JOIN,Outer\n" +
                               "||GATHER,Outer\n" +
                               "|||JOIN,Outer\n" +
                               "||||TAB,char_name,Outer\n" +
                               "||||HASH,Inner\n" +
                               "|||||JOIN,Outer\n" +
                               "||||||TAB,title,Outer\n" +
                               "||||||HASH,Inner\n" +
                               "|||||||JOIN,Outer\n" +
                               "||||||||JOIN,Outer\n" +
                               "|||||||||TAB,cast_info,Outer\n" +
                               "|||||||||HASH,Inner\n" +
                               "||||||||||TAB,role_type,Outer\n" +
                               "||||||||HASH,Inner\n" +
                               "|||||||||JOIN,Outer\n" +
                               "||||||||||TAB,movie_companies,Outer\n" +
                               "||||||||||HASH,Inner\n" +
                               "|||||||||||TAB,company_name,Outer\n" +
                               "||TAB,company_type,Inner";
        String postgresPlan = generatePostgresPlanJson(specification);
        String expectedPostgresPlan = "[{\"Plan\":{\"Node Type\":\"Aggregate\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Gather\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"char_name\",\"Plan Rows\":0,\"Actual Rows\":0},{\"Node Type\":\"Hash\",\"Parent Relationship\":\"Inner\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"title\",\"Plan Rows\":0,\"Actual Rows\":0},{\"Node Type\":\"Hash\",\"Parent Relationship\":\"Inner\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"cast_info\",\"Plan Rows\":0,\"Actual Rows\":0},{\"Node Type\":\"Hash\",\"Parent Relationship\":\"Inner\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"role_type\",\"Plan Rows\":0,\"Actual Rows\":0}]}]},{\"Node Type\":\"Hash\",\"Parent Relationship\":\"Inner\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Hash Join\",\"Parent Relationship\":\"Outer\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"movie_companies\",\"Plan Rows\":0,\"Actual Rows\":0},{\"Node Type\":\"Hash\",\"Parent Relationship\":\"Inner\",\"Plan Rows\":0,\"Actual Rows\":0,\"Plans\":[{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Outer\",\"Relation Name\":\"company_name\",\"Plan Rows\":0,\"Actual Rows\":0}]}]}]}]}]}]}]}]}]},{\"Node Type\":\"Seq Scan\",\"Parent Relationship\":\"Inner\",\"Relation Name\":\"company_type\",\"Plan Rows\":0,\"Actual Rows\":0}]}]}}]";
        assertEquals(expectedPostgresPlan, postgresPlan);
        checkPlanCanBeBuilt(postgresPlan);
    }
}
