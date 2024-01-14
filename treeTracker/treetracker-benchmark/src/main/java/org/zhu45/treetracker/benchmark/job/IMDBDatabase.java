package org.zhu45.treetracker.benchmark.job;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.zhu45.treetracker.benchmark.Database.getTableNode;

public class IMDBDatabase
{
    private IMDBDatabase()
    {
    }

    private static final String imdbIntSchemaName = "imdb_int";
    private static final String imdbSchemaName = "imdb";

    public static final String akaName = "aka_name";
    public static final String akaTitle = "aka_title";
    public static final String castInfo = "cast_info";
    public static final String charName = "char_name";
    public static final String compCastType = "comp_cast_type";
    public static final String companyName = "company_name";
    public static final String companyType = "company_type";
    public static final String completeCast = "complete_cast";
    public static final String infoType = "info_type";
    public static final String keyword = "keyword";
    public static final String kindType = "kind_type";
    public static final String linkType = "link_type";
    public static final String movieCompanies = "movie_companies";
    public static final String movieInfoIdx = "movie_info_idx";
    public static final String movieKeyword = "movie_keyword";
    public static final String movieLink = "movie_link";
    public static final String name = "name";
    public static final String roleType = "role_type";
    public static final String title = "title";
    public static final String movieInfo = "movie_info";
    public static final String personInfo = "person_info";

    public static class CodeGen
    {
        public static final Map<String, String> relation2MultiwayJoinNode = Map.ofEntries(
                entry(movieCompanies, "movieCompaniesNode"),
                entry(movieCompanies + "1", "movieCompaniesNode1"),
                entry(movieCompanies + "2", "movieCompaniesNode2"),
                entry(companyType, "companyTypeNode"),
                entry(akaTitle, "akaTitleNode"),
                entry(title, "titleNode"),
                entry(title + "1", "titleNode1"),
                entry(title + "2", "titleNode2"),
                entry(movieInfoIdx, "movieInfoIdxNode"),
                entry(movieInfoIdx + "1", "movieInfoIdxNode1"),
                entry(movieInfoIdx + "2", "movieInfoIdxNode2"),
                entry(infoType, "infoTypeNode"),
                entry(infoType + "1", "infoTypeNode1"),
                entry(infoType + "2", "infoTypeNode2"),
                entry(akaName, "akaNameNode"),
                entry(name, "nameNode"),
                entry(castInfo, "castInfoNode"),
                entry(companyName, "companyNameNode"),
                entry(companyName + "1", "companyNameNode1"),
                entry(companyName + "2", "companyNameNode2"),
                entry(roleType, "roleTypeNode"),
                entry(movieInfo, "movieInfoNode"),
                entry(movieKeyword, "movieKeywordNode"),
                entry(keyword, "keywordNode"),
                entry(movieLink, "movieLinkNode"),
                entry(linkType, "linkTypeNode"),
                entry(charName, "charNameNode"),
                entry(personInfo, "personInfoNode"),
                entry(kindType, "kindTypeNode"),
                entry(kindType + "1", "kindTypeNode1"),
                entry(kindType + "2", "kindTypeNode2"),
                entry(completeCast, "completeCastNode"),
                entry(compCastType, "compCastTypeNode"),
                entry(compCastType + "1", "compCastTypeNode1"),
                entry(compCastType + "2", "compCastTypeNode2"));
        public static final Map<String, String> queryName2SavePath = Map.<String, String>ofEntries(
                entry("Query1a", "q1"),
                entry("Query1b", "q1"),
                entry("Query1c", "q1"),
                entry("Query1d", "q1"),
                entry("Query2a", "q2"),
                entry("Query2b", "q2"),
                entry("Query2c", "q2"),
                entry("Query2d", "q2"),
                entry("Query3a", "q3"),
                entry("Query3b", "q3"),
                entry("Query3c", "q3"),
                entry("Query4a", "q4"),
                entry("Query4b", "q4"),
                entry("Query4c", "q4"),
                entry("Query5a", "q5"),
                entry("Query5b", "q5"),
                entry("Query5c", "q5"),
                entry("Query6a", "q6"),
                entry("Query6b", "q6"),
                entry("Query6c", "q6"),
                entry("Query6d", "q6"),
                entry("Query6e", "q6"),
                entry("Query6f", "q6"),
                entry("Query7a", "q7"),
                entry("Query7b", "q7"),
                entry("Query7c", "q7"),
                entry("Query8a", "q8"),
                entry("Query8b", "q8"),
                entry("Query8c", "q8"),
                entry("Query8d", "q8"),
                entry("Query9a", "q9"),
                entry("Query9b", "q9"),
                entry("Query9c", "q9"),
                entry("Query9d", "q9"),
                entry("Query10a", "q10"),
                entry("Query10b", "q10"),
                entry("Query10c", "q10"),
                entry("Query11a", "q11"),
                entry("Query11b", "q11"),
                entry("Query11c", "q11"),
                entry("Query11d", "q11"),
                entry("Query12a", "q12"),
                entry("Query12b", "q12"),
                entry("Query12c", "q12"),
                entry("Query13a", "q13"),
                entry("Query13b", "q13"),
                entry("Query13c", "q13"),
                entry("Query13d", "q13"),
                entry("Query14a", "q14"),
                entry("Query14b", "q14"),
                entry("Query14c", "q14"),
                entry("Query15a", "q15"),
                entry("Query15b", "q15"),
                entry("Query15c", "q15"),
                entry("Query15d", "q15"),
                entry("Query16a", "q16"),
                entry("Query16b", "q16"),
                entry("Query16c", "q16"),
                entry("Query16d", "q16"),
                entry("Query17a", "q17"),
                entry("Query17b", "q17"),
                entry("Query17c", "q17"),
                entry("Query17d", "q17"),
                entry("Query17e", "q17"),
                entry("Query17f", "q17"),
                entry("Query18a", "q18"),
                entry("Query18b", "q18"),
                entry("Query18c", "q18"),
                entry("Query19a", "q19"),
                entry("Query19b", "q19"),
                entry("Query19c", "q19"),
                entry("Query19d", "q19"),
                entry("Query20a", "q20"),
                entry("Query20b", "q20"),
                entry("Query20c", "q20"),
                entry("Query21a", "q21"),
                entry("Query21b", "q21"),
                entry("Query21c", "q21"),
                entry("Query22a", "q22"),
                entry("Query22b", "q22"),
                entry("Query22c", "q22"),
                entry("Query22d", "q22"),
                entry("Query23a", "q23"),
                entry("Query23b", "q23"),
                entry("Query23c", "q23"),
                entry("Query24a", "q24"),
                entry("Query24b", "q24"),
                entry("Query25a", "q25"),
                entry("Query25b", "q25"),
                entry("Query25c", "q25"),
                entry("Query26a", "q26"),
                entry("Query26b", "q26"),
                entry("Query26c", "q26"),
                entry("Query27a", "q27"),
                entry("Query27b", "q27"),
                entry("Query27c", "q27"),
                entry("Query28a", "q28"),
                entry("Query28b", "q28"),
                entry("Query28c", "q28"),
                entry("Query29a", "q29"),
                entry("Query29b", "q29"),
                entry("Query29c", "q29"),
                entry("Query30a", "q30"),
                entry("Query30b", "q30"),
                entry("Query30c", "q30"),
                entry("Query31a", "q31"),
                entry("Query31b", "q31"),
                entry("Query31c", "q31"),
                entry("Query32a", "q32"),
                entry("Query32b", "q32"),
                entry("Query33a", "q33"),
                entry("Query33b", "q33"),
                entry("Query33c", "q33"));

        public static final String templatePathPrefix = Paths.get("codegen", "job", "templates").toString();

        public static final Map<String, String> queryName2Template = createQueryName2Template(new ArrayList<>(queryName2SavePath.keySet()), ".javat");

        public static final Map<String, String> queryName2TempateYannakakis = createQueryName2Template(new ArrayList<>(queryName2SavePath.keySet()), "OptJoinTreeOptOrderingY.javat");

        public static Map<String, String> createQueryName2Template(List<String> queries, String extension)
        {
            Map<String, String> myMap = new HashMap<>();
            for (String query : queries) {
                myMap.put(query, query + extension);
            }
            return myMap;
        }

        public static final String savePathPrefix = Paths.get("treetracker-benchmark", "src", "main", "java", "org", "zhu45", "treetracker", "benchmark", "job").toString();
    }

    public static MultiwayJoinNode getAkaNameInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q7a:
            case Q7b:
            case Q7c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, akaName, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, akaName));
        }
    }

    public static MultiwayJoinNode getAkaTitleInt()
    {
        return getTableNode(new SchemaTableName(imdbIntSchemaName, akaTitle));
    }

    public static MultiwayJoinNode getCastInfoInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q8a:
            case Q8b:
            case Q9a:
            case Q9b:
            case Q9c:
            case Q9d:
            case Q10a:
            case Q10b:
            case Q10c:
            case Q18a:
            case Q18b:
            case Q18c:
            case Q19a:
            case Q19b:
            case Q19c:
            case Q19d:
            case Q24a:
            case Q24b:
            case Q25a:
            case Q25b:
            case Q25c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q30a:
            case Q30b:
            case Q30c:
            case Q31a:
            case Q31b:
            case Q31c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, castInfo, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, castInfo));
        }
    }

    public static MultiwayJoinNode getCharNameInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q20a:
            case Q20b:
            case Q20c:
            case Q26a:
            case Q26b:
            case Q26c:
            case Q29a:
            case Q29b:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, charName, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, charName));
        }
    }

    public static MultiwayJoinNode getCompCastTypeInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q20a:
            case Q20b:
            case Q20c:
            case Q23a:
            case Q23b:
            case Q23c:
            case Q26a:
            case Q26b:
            case Q26c:
            case Q27a:
            case Q27b:
            case Q27c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q30a:
            case Q30b:
            case Q30c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, compCastType, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, compCastType));
        }
    }

    public static MultiwayJoinNode getCompanyNameInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q2a:
            case Q2b:
            case Q2c:
            case Q2d:
            case Q8a:
            case Q8b:
            case Q8c:
            case Q8d:
            case Q9a:
            case Q9b:
            case Q9c:
            case Q9d:
            case Q10a:
            case Q10b:
            case Q10c:
            case Q11a:
            case Q11b:
            case Q11c:
            case Q11d:
            case Q12a:
            case Q12b:
            case Q12c:
            case Q13a:
            case Q13b:
            case Q13c:
            case Q13d:
            case Q15a:
            case Q15b:
            case Q15c:
            case Q15d:
            case Q16a:
            case Q16b:
            case Q16c:
            case Q16d:
            case Q17a:
            case Q17e:
            case Q19a:
            case Q19b:
            case Q19c:
            case Q19d:
            case Q21a:
            case Q21b:
            case Q21c:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q22d:
            case Q23a:
            case Q23b:
            case Q23c:
            case Q24a:
            case Q24b:
            case Q27a:
            case Q27b:
            case Q27c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q31a:
            case Q31b:
            case Q31c:
            case Q33a:
            case Q33b:
            case Q33c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, companyName, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, companyName));
        }
    }

    public static MultiwayJoinNode getCompanyTypeInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q1a:
            case Q1b:
            case Q1c:
            case Q1d:
            case Q5a:
            case Q5b:
            case Q5c:
            case Q11a:
            case Q11b:
            case Q11c:
            case Q11d:
            case Q12a:
            case Q12b:
            case Q12c:
            case Q13a:
            case Q13b:
            case Q13c:
            case Q13d:
            case Q21a:
            case Q21b:
            case Q21c:
            case Q27a:
            case Q27b:
            case Q27c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, companyType, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, companyType));
        }
    }

    public static MultiwayJoinNode getCompleteCastInt()
    {
        return getTableNode(new SchemaTableName(imdbIntSchemaName, completeCast));
    }

    public static MultiwayJoinNode getInfoTypeInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q1a:
            case Q1b:
            case Q1c:
            case Q1d:
            case Q4a:
            case Q4b:
            case Q4c:
            case Q7a:
            case Q7b:
            case Q7c:
            case Q12a:
            case Q12b:
            case Q12c:
            case Q13a:
            case Q13b:
            case Q13c:
            case Q13d:
            case Q14a:
            case Q14b:
            case Q14c:
            case Q15a:
            case Q15b:
            case Q15c:
            case Q15d:
            case Q18a:
            case Q18b:
            case Q18c:
            case Q19a:
            case Q19b:
            case Q19c:
            case Q19d:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q22d:
            case Q23a:
            case Q23b:
            case Q23c:
            case Q24a:
            case Q24b:
            case Q25a:
            case Q25b:
            case Q25c:
            case Q26a:
            case Q26b:
            case Q26c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q30a:
            case Q30b:
            case Q30c:
            case Q31a:
            case Q31b:
            case Q31c:
            case Q33a:
            case Q33b:
            case Q33c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, infoType, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, infoType));
        }
    }

    public static MultiwayJoinNode getKeywordInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q2a:
            case Q2b:
            case Q2c:
            case Q2d:
            case Q3a:
            case Q3b:
            case Q3c:
            case Q4a:
            case Q4b:
            case Q4c:
            case Q6a:
            case Q6b:
            case Q6c:
            case Q6d:
            case Q6e:
            case Q6f:
            case Q11a:
            case Q11b:
            case Q11c:
            case Q11d:
            case Q14a:
            case Q14b:
            case Q14c:
            case Q16a:
            case Q16b:
            case Q16c:
            case Q16d:
            case Q17a:
            case Q17b:
            case Q17c:
            case Q17d:
            case Q17e:
            case Q17f:
            case Q20a:
            case Q20b:
            case Q20c:
            case Q21a:
            case Q21b:
            case Q21c:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q22d:
            case Q23b:
            case Q24a:
            case Q24b:
            case Q25a:
            case Q25b:
            case Q25c:
            case Q26a:
            case Q26b:
            case Q26c:
            case Q27a:
            case Q27b:
            case Q27c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q30a:
            case Q30b:
            case Q30c:
            case Q31a:
            case Q31b:
            case Q31c:
            case Q32a:
            case Q32b:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, keyword, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, keyword));
        }
    }

    public static MultiwayJoinNode getKindTypeInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q13a:
            case Q13b:
            case Q13c:
            case Q13d:
            case Q14a:
            case Q14b:
            case Q14c:
            case Q20a:
            case Q20b:
            case Q20c:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q22d:
            case Q23a:
            case Q23b:
            case Q23c:
            case Q26a:
            case Q26b:
            case Q26c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q33a:
            case Q33b:
            case Q33c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, kindType, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, kindType));
        }
    }

    public static MultiwayJoinNode getLinkTypeInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q7a:
            case Q7b:
            case Q7c:
            case Q11a:
            case Q11b:
            case Q21a:
            case Q21b:
            case Q21c:
            case Q27a:
            case Q27b:
            case Q27c:
            case Q33a:
            case Q33b:
            case Q33c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, linkType, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, linkType));
        }
    }

    public static MultiwayJoinNode getMovieCompaniesInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q1a:
            case Q1b:
            case Q1c:
            case Q1d:
            case Q5a:
            case Q5b:
            case Q5c:
            case Q8a:
            case Q8b:
            case Q9a:
            case Q9b:
            case Q11a:
            case Q11b:
            case Q11c:
            case Q11d:
            case Q15a:
            case Q15b:
            case Q19a:
            case Q19b:
            case Q21a:
            case Q21b:
            case Q21c:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q27a:
            case Q27b:
            case Q27c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q31b:
            case Q33a:
            case Q33b:
            case Q33c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, movieCompanies, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, movieCompanies));
        }
    }

    public static MultiwayJoinNode getMovieInfoIdxInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q4a:
            case Q4b:
            case Q4c:
            case Q12a:
            case Q12b:
            case Q12c:
            case Q14a:
            case Q14b:
            case Q14c:
            case Q18a:
            case Q18b:
            case Q18c:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q22d:
            case Q25a:
            case Q25b:
            case Q25c:
            case Q26a:
            case Q26b:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q30a:
            case Q30b:
            case Q30c:
            case Q31a:
            case Q31b:
            case Q31c:
            case Q33a:
            case Q33b:
            case Q33c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, movieInfoIdx, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, movieInfoIdx));
        }
    }

    public static MultiwayJoinNode getMovieKeywordInt()
    {
        return getTableNode(new SchemaTableName(imdbIntSchemaName, movieKeyword));
    }

    public static MultiwayJoinNode getMovieLinkInt()
    {
        return getTableNode(new SchemaTableName(imdbIntSchemaName, movieLink));
    }

    public static MultiwayJoinNode getMovieLinkInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q7a:
            case Q7b:
            case Q7c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, movieLink, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, movieLink));
        }
    }

    public static MultiwayJoinNode getName()
    {
        return getTableNode(new SchemaTableName(imdbSchemaName, name));
    }

    public static MultiwayJoinNode getNameInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q6a:
            case Q6b:
            case Q6c:
            case Q6d:
            case Q6e:
            case Q7a:
            case Q7b:
            case Q7c:
            case Q8a:
            case Q8b:
            case Q9a:
            case Q9b:
            case Q9c:
            case Q9d:
            case Q17a:
            case Q17b:
            case Q17c:
            case Q17d:
            case Q17f:
            case Q18a:
            case Q18b:
            case Q18c:
            case Q19a:
            case Q19b:
            case Q19c:
            case Q19d:
            case Q20b:
            case Q24a:
            case Q24b:
            case Q25a:
            case Q25b:
            case Q25c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q30a:
            case Q30b:
            case Q30c:
            case Q31b:
            case Q31a:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, name, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, name));
        }
    }

    public static MultiwayJoinNode getRoleTypeInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q8a:
            case Q8b:
            case Q8c:
            case Q8d:
            case Q9a:
            case Q9b:
            case Q9c:
            case Q9d:
            case Q10a:
            case Q10b:
            case Q19a:
            case Q19b:
            case Q19c:
            case Q19d:
            case Q24a:
            case Q24b:
            case Q29a:
            case Q29b:
            case Q29c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, roleType, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, roleType));
        }
    }

    public static MultiwayJoinNode getTitle()
    {
        return getTableNode(new SchemaTableName(imdbSchemaName, title));
    }

    public static MultiwayJoinNode getTitleInt(JOBQueries jobQueries, TableInstanceId tableInstanceId)
    {
        switch (jobQueries) {
            case Q1b:
            case Q1c:
            case Q1d:
            case Q3a:
            case Q3b:
            case Q3c:
            case Q4a:
            case Q4b:
            case Q4c:
            case Q5a:
            case Q5b:
            case Q5c:
            case Q6a:
            case Q6b:
            case Q6c:
            case Q6d:
            case Q6e:
            case Q6f:
            case Q7a:
            case Q7b:
            case Q7c:
            case Q8b:
            case Q9a:
            case Q9b:
            case Q10a:
            case Q10b:
            case Q10c:
            case Q11a:
            case Q11b:
            case Q11c:
            case Q11d:
            case Q12a:
            case Q12b:
            case Q12c:
            case Q13b:
            case Q13c:
            case Q14a:
            case Q14b:
            case Q14c:
            case Q15a:
            case Q15b:
            case Q15c:
            case Q15d:
            case Q16a:
            case Q16c:
            case Q16d:
            case Q18b:
            case Q19a:
            case Q19b:
            case Q19c:
            case Q19d:
            case Q20a:
            case Q20b:
            case Q20c:
            case Q21a:
            case Q21b:
            case Q21c:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q22d:
            case Q23a:
            case Q23b:
            case Q23c:
            case Q24a:
            case Q24b:
            case Q25b:
            case Q26a:
            case Q26b:
            case Q26c:
            case Q27a:
            case Q27b:
            case Q27c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q30a:
            case Q30b:
            case Q31b:
            case Q32a:
            case Q32b:
            case Q33a:
            case Q33b:
            case Q33c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, title, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, title));
        }
    }

    public static MultiwayJoinNode getMovieInfoInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q3a:
            case Q3b:
            case Q3c:
            case Q5a:
            case Q5b:
            case Q5c:
            case Q12a:
            case Q12c:
            case Q13a:
            case Q13b:
            case Q13c:
            case Q13d:
            case Q14a:
            case Q14b:
            case Q14c:
            case Q15a:
            case Q15b:
            case Q15c:
            case Q15d:
            case Q18b:
            case Q18c:
            case Q19a:
            case Q19b:
            case Q19c:
            case Q21a:
            case Q21b:
            case Q21c:
            case Q22a:
            case Q22b:
            case Q22c:
            case Q22d:
            case Q23a:
            case Q23b:
            case Q23c:
            case Q24a:
            case Q24b:
            case Q25a:
            case Q25b:
            case Q25c:
            case Q27a:
            case Q27b:
            case Q27c:
            case Q28a:
            case Q28b:
            case Q28c:
            case Q29a:
            case Q29b:
            case Q29c:
            case Q30a:
            case Q30b:
            case Q30c:
            case Q31b:
            case Q31a:
            case Q31c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, movieInfo, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, movieInfo));
        }
    }

    public static MultiwayJoinNode getPersonInfoInt(JOBQueries jobQueries)
    {
        switch (jobQueries) {
            case Q7a:
            case Q7b:
            case Q7c:
            case Q29a:
            case Q29b:
            case Q29c:
                return getTableNode(new SchemaTableName(imdbSchemaName, constructViewName(jobQueries, personInfo, null)));
            default:
                return getTableNode(new SchemaTableName(imdbIntSchemaName, personInfo));
        }
    }

    private static String constructViewName(JOBQueries jobQueries, String tableName, TableInstanceId tableInstanceId)
    {
        if (tableInstanceId != null) {
            return jobQueries.name().toLowerCase() + "_" + tableName + tableInstanceId.getTableInstanceId();
        }
        else {
            return jobQueries.name().toLowerCase() + "_" + tableName;
        }
    }
}
