package org.zhu45.treetracker.benchmark.job;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.q1.Query13;
import org.zhu45.treetracker.benchmark.job.q1.Query1a;
import org.zhu45.treetracker.benchmark.job.q1.Query1aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q1.Query1aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1b;
import org.zhu45.treetracker.benchmark.job.q1.Query1bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q1.Query1bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1c;
import org.zhu45.treetracker.benchmark.job.q1.Query1cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q1.Query1cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1d;
import org.zhu45.treetracker.benchmark.job.q1.Query1dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q1.Query1dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query103;
import org.zhu45.treetracker.benchmark.job.q10.Query10a;
import org.zhu45.treetracker.benchmark.job.q10.Query10aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q10.Query10b;
import org.zhu45.treetracker.benchmark.job.q10.Query10bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q10.Query10c;
import org.zhu45.treetracker.benchmark.job.q10.Query10cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11;
import org.zhu45.treetracker.benchmark.job.q11.Query11a;
import org.zhu45.treetracker.benchmark.job.q11.Query11aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11b;
import org.zhu45.treetracker.benchmark.job.q11.Query11bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11c;
import org.zhu45.treetracker.benchmark.job.q11.Query11cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11d;
import org.zhu45.treetracker.benchmark.job.q11.Query11dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q12.Query12;
import org.zhu45.treetracker.benchmark.job.q12.Query12a;
import org.zhu45.treetracker.benchmark.job.q12.Query12aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q12.Query12b;
import org.zhu45.treetracker.benchmark.job.q12.Query12bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q12.Query12c;
import org.zhu45.treetracker.benchmark.job.q12.Query12cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q13.Query13a;
import org.zhu45.treetracker.benchmark.job.q13.Query13b;
import org.zhu45.treetracker.benchmark.job.q13.Query13c;
import org.zhu45.treetracker.benchmark.job.q13.Query13d;
import org.zhu45.treetracker.benchmark.job.q14.Query146;
import org.zhu45.treetracker.benchmark.job.q14.Query14a;
import org.zhu45.treetracker.benchmark.job.q14.Query14b;
import org.zhu45.treetracker.benchmark.job.q14.Query14c;
import org.zhu45.treetracker.benchmark.job.q15.Query153;
import org.zhu45.treetracker.benchmark.job.q15.Query15a;
import org.zhu45.treetracker.benchmark.job.q15.Query15b;
import org.zhu45.treetracker.benchmark.job.q15.Query15c;
import org.zhu45.treetracker.benchmark.job.q15.Query15d;
import org.zhu45.treetracker.benchmark.job.q16.Query161;
import org.zhu45.treetracker.benchmark.job.q16.Query16a;
import org.zhu45.treetracker.benchmark.job.q16.Query16b;
import org.zhu45.treetracker.benchmark.job.q16.Query16c;
import org.zhu45.treetracker.benchmark.job.q16.Query16d;
import org.zhu45.treetracker.benchmark.job.q17.Query172;
import org.zhu45.treetracker.benchmark.job.q17.Query17a;
import org.zhu45.treetracker.benchmark.job.q17.Query17b;
import org.zhu45.treetracker.benchmark.job.q17.Query17c;
import org.zhu45.treetracker.benchmark.job.q17.Query17d;
import org.zhu45.treetracker.benchmark.job.q17.Query17e;
import org.zhu45.treetracker.benchmark.job.q17.Query17f;
import org.zhu45.treetracker.benchmark.job.q18.Query181;
import org.zhu45.treetracker.benchmark.job.q18.Query18a;
import org.zhu45.treetracker.benchmark.job.q18.Query18b;
import org.zhu45.treetracker.benchmark.job.q18.Query18c;
import org.zhu45.treetracker.benchmark.job.q19.Query191;
import org.zhu45.treetracker.benchmark.job.q19.Query19a;
import org.zhu45.treetracker.benchmark.job.q19.Query19aEstimationTroubleShooting;
import org.zhu45.treetracker.benchmark.job.q19.Query19b;
import org.zhu45.treetracker.benchmark.job.q19.Query19c;
import org.zhu45.treetracker.benchmark.job.q19.Query19d;
import org.zhu45.treetracker.benchmark.job.q2.Query2;
import org.zhu45.treetracker.benchmark.job.q2.Query2a;
import org.zhu45.treetracker.benchmark.job.q2.Query2aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2b;
import org.zhu45.treetracker.benchmark.job.q2.Query2bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2c;
import org.zhu45.treetracker.benchmark.job.q2.Query2cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2d;
import org.zhu45.treetracker.benchmark.job.q2.Query2dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20;
import org.zhu45.treetracker.benchmark.job.q20.Query20a;
import org.zhu45.treetracker.benchmark.job.q20.Query20b;
import org.zhu45.treetracker.benchmark.job.q20.Query20c;
import org.zhu45.treetracker.benchmark.job.q21.Query211;
import org.zhu45.treetracker.benchmark.job.q21.Query21a;
import org.zhu45.treetracker.benchmark.job.q21.Query21b;
import org.zhu45.treetracker.benchmark.job.q21.Query21c;
import org.zhu45.treetracker.benchmark.job.q22.Query22;
import org.zhu45.treetracker.benchmark.job.q22.Query22a;
import org.zhu45.treetracker.benchmark.job.q22.Query22b;
import org.zhu45.treetracker.benchmark.job.q22.Query22c;
import org.zhu45.treetracker.benchmark.job.q22.Query22d;
import org.zhu45.treetracker.benchmark.job.q23.Query23;
import org.zhu45.treetracker.benchmark.job.q23.Query23a;
import org.zhu45.treetracker.benchmark.job.q23.Query23b;
import org.zhu45.treetracker.benchmark.job.q23.Query23c;
import org.zhu45.treetracker.benchmark.job.q24.Query24a;
import org.zhu45.treetracker.benchmark.job.q24.Query24b;
import org.zhu45.treetracker.benchmark.job.q25.Query25;
import org.zhu45.treetracker.benchmark.job.q25.Query25a;
import org.zhu45.treetracker.benchmark.job.q25.Query25b;
import org.zhu45.treetracker.benchmark.job.q25.Query25c;
import org.zhu45.treetracker.benchmark.job.q26.Query26;
import org.zhu45.treetracker.benchmark.job.q26.Query26a;
import org.zhu45.treetracker.benchmark.job.q26.Query26b;
import org.zhu45.treetracker.benchmark.job.q26.Query26c;
import org.zhu45.treetracker.benchmark.job.q27.Query273;
import org.zhu45.treetracker.benchmark.job.q27.Query27a;
import org.zhu45.treetracker.benchmark.job.q27.Query27b;
import org.zhu45.treetracker.benchmark.job.q27.Query27c;
import org.zhu45.treetracker.benchmark.job.q28.Query28;
import org.zhu45.treetracker.benchmark.job.q28.Query28a;
import org.zhu45.treetracker.benchmark.job.q28.Query28b;
import org.zhu45.treetracker.benchmark.job.q28.Query28c;
import org.zhu45.treetracker.benchmark.job.q29.Query29a;
import org.zhu45.treetracker.benchmark.job.q29.Query29b;
import org.zhu45.treetracker.benchmark.job.q29.Query29c;
import org.zhu45.treetracker.benchmark.job.q3.Query32;
import org.zhu45.treetracker.benchmark.job.q3.Query3a;
import org.zhu45.treetracker.benchmark.job.q3.Query3aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q3.Query3aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3b;
import org.zhu45.treetracker.benchmark.job.q3.Query3bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q3.Query3bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3c;
import org.zhu45.treetracker.benchmark.job.q3.Query3cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q3.Query3cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30;
import org.zhu45.treetracker.benchmark.job.q30.Query30a;
import org.zhu45.treetracker.benchmark.job.q30.Query30b;
import org.zhu45.treetracker.benchmark.job.q30.Query30c;
import org.zhu45.treetracker.benchmark.job.q31.Query311;
import org.zhu45.treetracker.benchmark.job.q31.Query31a;
import org.zhu45.treetracker.benchmark.job.q31.Query31b;
import org.zhu45.treetracker.benchmark.job.q31.Query31c;
import org.zhu45.treetracker.benchmark.job.q32.Query32a;
import org.zhu45.treetracker.benchmark.job.q32.Query32b;
import org.zhu45.treetracker.benchmark.job.q33.Query331;
import org.zhu45.treetracker.benchmark.job.q33.Query33a;
import org.zhu45.treetracker.benchmark.job.q33.Query33b;
import org.zhu45.treetracker.benchmark.job.q33.Query33c;
import org.zhu45.treetracker.benchmark.job.q4.Query44;
import org.zhu45.treetracker.benchmark.job.q4.Query4a;
import org.zhu45.treetracker.benchmark.job.q4.Query4aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q4.Query4aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4b;
import org.zhu45.treetracker.benchmark.job.q4.Query4bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q4.Query4bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4c;
import org.zhu45.treetracker.benchmark.job.q4.Query4cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q4.Query4cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query53;
import org.zhu45.treetracker.benchmark.job.q5.Query5a;
import org.zhu45.treetracker.benchmark.job.q5.Query5aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q5.Query5b;
import org.zhu45.treetracker.benchmark.job.q5.Query5bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q5.Query5bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5c;
import org.zhu45.treetracker.benchmark.job.q5.Query5cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q5.Query5cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query62;
import org.zhu45.treetracker.benchmark.job.q6.Query6a;
import org.zhu45.treetracker.benchmark.job.q6.Query6aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6b;
import org.zhu45.treetracker.benchmark.job.q6.Query6bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6c;
import org.zhu45.treetracker.benchmark.job.q6.Query6cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6d;
import org.zhu45.treetracker.benchmark.job.q6.Query6dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6e;
import org.zhu45.treetracker.benchmark.job.q6.Query6eFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6eOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6f;
import org.zhu45.treetracker.benchmark.job.q6.Query6fFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q7.Query7;
import org.zhu45.treetracker.benchmark.job.q7.Query7a;
import org.zhu45.treetracker.benchmark.job.q7.Query7aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q7.Query7aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7b;
import org.zhu45.treetracker.benchmark.job.q7.Query7bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q7.Query7c;
import org.zhu45.treetracker.benchmark.job.q7.Query7cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query84;
import org.zhu45.treetracker.benchmark.job.q8.Query8a;
import org.zhu45.treetracker.benchmark.job.q8.Query8aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query8b;
import org.zhu45.treetracker.benchmark.job.q8.Query8bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query8c;
import org.zhu45.treetracker.benchmark.job.q8.Query8cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query8cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8d;
import org.zhu45.treetracker.benchmark.job.q8.Query8dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9;
import org.zhu45.treetracker.benchmark.job.q9.Query9a;
import org.zhu45.treetracker.benchmark.job.q9.Query9aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9b;
import org.zhu45.treetracker.benchmark.job.q9.Query9bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9c;
import org.zhu45.treetracker.benchmark.job.q9.Query9cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9d;
import org.zhu45.treetracker.benchmark.job.q9.Query9dFindOptJoinTree;

import java.util.HashMap;
import java.util.Map;

public enum JOBQueries
        implements QueryEnum
{
    Q1("Q1"),
    Q1a("Q1a"),
    Q1aFindOptJoinTree("Q1aFindOptJoinTree"),
    Q1aOptJoinTreeOptOrdering("Q1aOptJoinTreeOptOrdering"),
    Q1b("Q1b"),
    Q1bFindOptJoinTree("Q1bFindOptJoinTree"),
    Q1bOptJoinTreeOptOrdering("Q1bOptJoinTreeOptOrdering"),
    Q1c("Q1c"),
    Q1cFindOptJoinTree("Q1cFindOptJoinTree"),
    Q1cOptJoinTreeOptOrdering("Q1cOptJoinTreeOptOrdering"),
    Q1d("Q1d"),
    Q1dFindOptJoinTree("Q1dFindOptJoinTree"),
    Q1dOptJoinTreeOptOrdering("Q1dOptJoinTreeOptOrdering"),
    Q2("Q2"),
    Q2a("Q2a"),
    Q2aFindOptJoinTree("Q2aFindOptJoinTree"),
    Q2aOptJoinTreeOptOrdering("Q2aOptJoinTreeOptOrdering"),
    Q2b("Q2b"),
    Q2bFindOptJoinTree("Q2bFindOptJoinTree"),
    Q2bOptJoinTreeOptOrdering("Q2bOptJoinTreeOptOrdering"),
    Q2c("Q2c"),
    Q2cFindOptJoinTree("Q2cFindOptJoinTree"),
    Q2cOptJoinTreeOptOrdering("Q2cOptJoinTreeOptOrdering"),
    Q2d("Q2d"),
    Q2dFindOptJoinTree("Q2dFindOptJoinTree"),
    Q2dOptJoinTreeOptOrdering("Q2dOptJoinTreeOptOrdering"),
    Q3("Q3"),
    Q3a("Q3a"),
    Q3aFindOptJoinTree("Q3aFindOptJoinTree"),
    Q3aOptJoinTreeOptOrdering("Q3aOptJoinTreeOptOrdering"),
    Q3b("Q3b"),
    Q3bFindOptJoinTree("Q3bFindOptJoinTree"),
    Q3bOptJoinTreeOptOrdering("Q3bOptJoinTreeOptOrdering"),
    Q3c("Q3c"),
    Q3cFindOptJoinTree("Q3cFindOptJoinTree"),
    Q3cOptJoinTreeOptOrdering("Q3cOptJoinTreeOptOrdering"),
    Q4("Q4"),
    Q4a("Q4a"),
    Q4aFindOptJoinTree("Q4aFindOptJoinTree"),
    Q4aOptJoinTreeOptOrdering("Q4aOptJoinTreeOptOrdering"),
    Q4b("Q4b"),
    Q4bFindOptJoinTree("Q4bFindOptJoinTree"),
    Q4bOptJoinTreeOptOrdering("Q4bOptJoinTreeOptOrdering"),
    Q4c("Q4c"),
    Q4cFindOptJoinTree("Q4cFindOptJoinTree"),
    Q4cOptJoinTreeOptOrdering("Q4cOptJoinTreeOptOrdering"),
    Q5("Q5"),
    Q5a("Q5a"),
    Q5aFindOptJoinTree("Q5aFindOptJoinTree"),
    Q5aOptJoinTreeOptOrdering("Q5aOptJoinTreeOptOrdering"),
    Q5b("Q5b"),
    Q5bFindOptJoinTree("Q5bFindOptJoinTree"),
    Q5bOptJoinTreeOptOrdering("Q5bOptJoinTreeOptOrdering"),
    Q5c("Q5c"),
    Q5cFindOptJoinTree("Q5cFindOptJoinTree"),
    Q5cOptJoinTreeOptOrdering("Q5cOptJoinTreeOptOrdering"),
    Q6("Q6"),
    Q6a("Q6a"),
    Q6aFindOptJoinTree("Q6aFindOptJoinTree"),
    Q6aOptJoinTreeOptOrdering("Q6aOptJoinTreeOptOrdering"),
    Q6b("Q6b"),
    Q6bFindOptJoinTree("Q6bFindOptJoinTree"),
    Q6bOptJoinTreeOptOrdering("Q6bOptJoinTreeOptOrdering"),
    Q6c("Q6c"),
    Q6cFindOptJoinTree("Q6cFindOptJoinTree"),
    Q6cOptJoinTreeOptOrdering("Q6cOptJoinTreeOptOrdering"),
    Q6d("Q6d"),
    Q6dFindOptJoinTree("Q6dFindOptJoinTree"),
    Q6dOptJoinTreeOptOrdering("Q6dOptJoinTreeOptOrdering"),
    Q6e("Q6e"),
    Q6eFindOptJoinTree("Q6eFindOptJoinTree"),
    Q6eOptJoinTreeOptOrdering("Q6eOptJoinTreeOptOrdering"),
    Q6f("Q6f"),
    Q6fFindOptJoinTree("Q6fFindOptJoinTree"),
    Q7("Q7"),
    Q7a("Q7a"),
    Q7aFindOptJoinTree("Q7aFindOptJoinTree"),
    Q7aOptJoinTreeOptOrdering("Q7aOptJoinTreeOptOrdering"),
    Q7b("q7b"),
    Q7bFindOptJoinTree("Q7bFindOptJoinTree"),
    Q7c("Q7c"),
    Q7cFindOptJoinTree("Q7cFindOptJoinTree"),
    Q8("Q8"),
    Q8a("Q8a"),
    Q8aFindOptJoinTree("Q8aFindOptJoinTree"),
    Q8b("Q8b"),
    Q8bFindOptJoinTree("Q8bFindOptJoinTree"),
    Q8c("Q8c"),
    Q8cFindOptJoinTree("Q8cFindOptJoinTree"),
    Q8cOptJoinTreeOptOrdering("Q8cOptJoinTreeOptOrdering"),
    Q8d("Q8d"),
    Q8dFindOptJoinTree("Q8dFindOptJoinTree"),
    Q9("Q9"),
    Q9a("Q9a"),
    Q9aFindOptJoinTree("Q9aFindOptJoinTree"),
    Q9b("Q9b"),
    Q9bFindOptJoinTree("Q9bFindOptJoinTree"),
    Q9c("Q9c"),
    Q9cFindOptJoinTree("Q9cFindOptJoinTree"),
    Q9d("Q9d"),
    Q9dFindOptJoinTree("Q9dFindOptJoinTree"),
    Q10("Q10"),
    Q10a("Q10a"),
    Q10aFindOptJoinTree("Q10aFindOptJoinTree"),
    Q10b("Q10b"),
    Q10bFindOptJoinTree("Q10bFindOptJoinTree"),
    Q10c("Q10c"),
    Q10cFindOptJoinTree("Q10cFindOptJoinTree"),
    Q11("Q11"),
    Q11a("Q11a"),
    Q11aFindOptJoinTree("Q11aFindOptJoinTree"),
    Q11b("Q11b"),
    Q11bFindOptJoinTree("Q11bFindOptJoinTree"),
    Q11c("Q11c"),
    Q11cFindOptJoinTree("Q11cFindOptJoinTree"),
    Q11d("Q11d"),
    Q11dFindOptJoinTree("Q11dFindOptJoinTree"),
    Q12("Q12"),
    Q12a("Q12a"),
    Q12aFindOptJoinTree("Q12aFindOptJoinTree"),
    Q12b("Q12b"),
    Q12bFindOptJoinTree("Q12bFindOptJoinTree"),
    Q12c("Q12c"),
    Q12cFindOptJoinTree("Q12cFindOptJoinTree"),
    Q13("Q13"),
    Q13a("Q13a"),
    Q13b("Q13b"),
    Q13c("Q13c"),
    Q13d("Q13d"),
    Q14("Q14"),
    Q14a("Q14a"),
    Q14b("Q14b"),
    Q14c("Q14c"),
    Q15("Q15"),
    Q15a("Q15a"),
    Q15b("Q15b"),
    Q15c("Q15c"),
    Q15d("Q15d"),
    Q16("Q16"),
    Q16a("Q16a"),
    Q16b("Q16b"),
    Q16c("Q16c"),
    Q16d("Q16d"),
    Q17("Q17"),
    Q17a("Q17a"),
    Q17b("Q17b"),
    Q17c("Q17c"),
    Q17d("Q17d"),
    Q17e("Q17e"),
    Q17f("Q17f"),
    Q18("Q18"),
    Q18a("Q18a"),
    Q18b("Q18b"),
    Q18c("Q18c"),
    Q19("Q19"),
    Q19a("Q19a"),
    Q19aEstimationTroubleShooting("Q19aEstimationTroubleShooting"),
    Q19b("Q19b"),
    Q19c("Q19c"),
    Q19d("Q19d"),
    Q20("Q20"),
    Q20a("Q20a"),
    Q20b("Q20b"),
    Q20c("Q20c"),
    Q21("Q21"),
    Q21a("Q21a"),
    Q21b("Q21b"),
    Q21c("Q21c"),
    Q22("Q22"),
    Q22a("Q22a"),
    Q22b("Q22b"),
    Q22c("Q22c"),
    Q22d("Q22d"),
    Q23("Q23"),
    Q23a("Q23a"),
    Q23b("Q23b"),
    Q23c("Q23c"),
    Q24("Q24"),
    Q24a("Q24a"),
    Q24b("Q24b"),
    Q25("Q25"),
    Q25a("Q25a"),
    Q25b("Q25b"),
    Q25c("Q24c"),
    Q26("Q26"),
    Q26a("Q26a"),
    Q26b("Q26b"),
    Q26c("Q26c"),
    Q27("Q27"),
    Q27a("Q27a"),
    Q27b("Q27b"),
    Q27c("Q27c"),
    Q28("Q28"),
    Q28a("Q28a"),
    Q28b("Q28b"),
    Q28c("Q28c"),
    Q29("Q29"),
    Q29a("Q29a"),
    Q29b("Q29b"),
    Q29c("Q29c"),
    Q30("Q30"),
    Q30a("Q30a"),
    Q30b("Q30b"),
    Q30c("Q30c"),
    Q31("Q31"),
    Q31a("Q31a"),
    Q31b("Q31b"),
    Q31c("Q31c"),
    Q32("Q32"),
    Q32a("Q32a"),
    Q32b("Q32b"),
    Q33("Q33"),
    Q33a("Q33a"),
    Q33b("Q33b"),
    Q33c("Q33c"),
    NOPREDICATE("NOPREDICATE");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (JOBQueries c : values()) {
            switch (c) {
                case Q1:
                    map.put(c.val, Query13.class);
                    break;
                case Q1a:
                    map.put(c.val, Query1a.class);
                    break;
                case Q1aFindOptJoinTree:
                    map.put(c.val, Query1aFindOptJoinTree.class);
                    break;
                case Q1aOptJoinTreeOptOrdering:
                    map.put(c.val, Query1aOptJoinTreeOptOrdering.class);
                    break;
                case Q1b:
                    map.put(c.val, Query1b.class);
                    break;
                case Q1bFindOptJoinTree:
                    map.put(c.val, Query1bFindOptJoinTree.class);
                    break;
                case Q1bOptJoinTreeOptOrdering:
                    map.put(c.val, Query1bOptJoinTreeOptOrdering.class);
                    break;
                case Q1c:
                    map.put(c.val, Query1c.class);
                    break;
                case Q1cFindOptJoinTree:
                    map.put(c.val, Query1cFindOptJoinTree.class);
                    break;
                case Q1cOptJoinTreeOptOrdering:
                    map.put(c.val, Query1cOptJoinTreeOptOrdering.class);
                    break;
                case Q1d:
                    map.put(c.val, Query1d.class);
                    break;
                case Q1dFindOptJoinTree:
                    map.put(c.val, Query1dFindOptJoinTree.class);
                    break;
                case Q1dOptJoinTreeOptOrdering:
                    map.put(c.val, Query1dOptJoinTreeOptOrdering.class);
                    break;
                case Q2:
                    map.put(c.val, Query2.class);
                    break;
                case Q2a:
                    map.put(c.val, Query2a.class);
                    break;
                case Q2aFindOptJoinTree:
                    map.put(c.val, Query2aFindOptJoinTree.class);
                    break;
                case Q2aOptJoinTreeOptOrdering:
                    map.put(c.val, Query2aOptJoinTreeOptOrdering.class);
                    break;
                case Q2b:
                    map.put(c.val, Query2b.class);
                    break;
                case Q2bFindOptJoinTree:
                    map.put(c.val, Query2bFindOptJoinTree.class);
                    break;
                case Q2bOptJoinTreeOptOrdering:
                    map.put(c.val, Query2bOptJoinTreeOptOrdering.class);
                    break;
                case Q2c:
                    map.put(c.val, Query2c.class);
                    break;
                case Q2cFindOptJoinTree:
                    map.put(c.val, Query2cFindOptJoinTree.class);
                    break;
                case Q2cOptJoinTreeOptOrdering:
                    map.put(c.val, Query2cOptJoinTreeOptOrdering.class);
                    break;
                case Q2d:
                    map.put(c.val, Query2d.class);
                    break;
                case Q2dFindOptJoinTree:
                    map.put(c.val, Query2dFindOptJoinTree.class);
                    break;
                case Q2dOptJoinTreeOptOrdering:
                    map.put(c.val, Query2dOptJoinTreeOptOrdering.class);
                    break;
                case Q3:
                    map.put(c.val, Query32.class);
                    break;
                case Q3a:
                    map.put(c.val, Query3a.class);
                    break;
                case Q3aFindOptJoinTree:
                    map.put(c.val, Query3aFindOptJoinTree.class);
                    break;
                case Q3aOptJoinTreeOptOrdering:
                    map.put(c.val, Query3aOptJoinTreeOptOrdering.class);
                    break;
                case Q3b:
                    map.put(c.val, Query3b.class);
                    break;
                case Q3bFindOptJoinTree:
                    map.put(c.val, Query3bFindOptJoinTree.class);
                    break;
                case Q3bOptJoinTreeOptOrdering:
                    map.put(c.val, Query3bOptJoinTreeOptOrdering.class);
                    break;
                case Q3c:
                    map.put(c.val, Query3c.class);
                    break;
                case Q3cFindOptJoinTree:
                    map.put(c.val, Query3cFindOptJoinTree.class);
                    break;
                case Q3cOptJoinTreeOptOrdering:
                    map.put(c.val, Query3cOptJoinTreeOptOrdering.class);
                    break;
                case Q4:
                    map.put(c.val, Query44.class);
                    break;
                case Q4a:
                    map.put(c.val, Query4a.class);
                    break;
                case Q4aFindOptJoinTree:
                    map.put(c.val, Query4aFindOptJoinTree.class);
                    break;
                case Q4aOptJoinTreeOptOrdering:
                    map.put(c.val, Query4aOptJoinTreeOptOrdering.class);
                    break;
                case Q4b:
                    map.put(c.val, Query4b.class);
                    break;
                case Q4bFindOptJoinTree:
                    map.put(c.val, Query4bFindOptJoinTree.class);
                    break;
                case Q4bOptJoinTreeOptOrdering:
                    map.put(c.val, Query4bOptJoinTreeOptOrdering.class);
                    break;
                case Q4c:
                    map.put(c.val, Query4c.class);
                    break;
                case Q4cFindOptJoinTree:
                    map.put(c.val, Query4cFindOptJoinTree.class);
                    break;
                case Q4cOptJoinTreeOptOrdering:
                    map.put(c.val, Query4cOptJoinTreeOptOrdering.class);
                    break;
                case Q5:
                    map.put(c.val, Query53.class);
                    break;
                case Q5a:
                    map.put(c.val, Query5a.class);
                    break;
                case Q5aFindOptJoinTree:
                    map.put(c.val, Query5aFindOptJoinTree.class);
                    break;
                case Q5b:
                    map.put(c.val, Query5b.class);
                    break;
                case Q5bFindOptJoinTree:
                    map.put(c.val, Query5bFindOptJoinTree.class);
                    break;
                case Q5bOptJoinTreeOptOrdering:
                    map.put(c.val, Query5bOptJoinTreeOptOrdering.class);
                    break;
                case Q5c:
                    map.put(c.val, Query5c.class);
                    break;
                case Q5cFindOptJoinTree:
                    map.put(c.val, Query5cFindOptJoinTree.class);
                    break;
                case Q5cOptJoinTreeOptOrdering:
                    map.put(c.val, Query5cOptJoinTreeOptOrdering.class);
                    break;
                case Q6:
                    map.put(c.val, Query62.class);
                    break;
                case Q6a:
                    map.put(c.val, Query6a.class);
                    break;
                case Q6aFindOptJoinTree:
                    map.put(c.val, Query6aFindOptJoinTree.class);
                    break;
                case Q6aOptJoinTreeOptOrdering:
                    map.put(c.val, Query6aOptJoinTreeOptOrdering.class);
                    break;
                case Q6b:
                    map.put(c.val, Query6b.class);
                    break;
                case Q6bFindOptJoinTree:
                    map.put(c.val, Query6bFindOptJoinTree.class);
                    break;
                case Q6bOptJoinTreeOptOrdering:
                    map.put(c.val, Query6bOptJoinTreeOptOrdering.class);
                    break;
                case Q6c:
                    map.put(c.val, Query6c.class);
                    break;
                case Q6cFindOptJoinTree:
                    map.put(c.val, Query6cFindOptJoinTree.class);
                    break;
                case Q6cOptJoinTreeOptOrdering:
                    map.put(c.val, Query6cOptJoinTreeOptOrdering.class);
                    break;
                case Q6d:
                    map.put(c.val, Query6d.class);
                    break;
                case Q6dFindOptJoinTree:
                    map.put(c.val, Query6dFindOptJoinTree.class);
                    break;
                case Q6dOptJoinTreeOptOrdering:
                    map.put(c.val, Query6dOptJoinTreeOptOrdering.class);
                    break;
                case Q6e:
                    map.put(c.val, Query6e.class);
                    break;
                case Q6eFindOptJoinTree:
                    map.put(c.val, Query6eFindOptJoinTree.class);
                    break;
                case Q6eOptJoinTreeOptOrdering:
                    map.put(c.val, Query6eOptJoinTreeOptOrdering.class);
                    break;
                case Q6f:
                    map.put(c.val, Query6f.class);
                    break;
                case Q6fFindOptJoinTree:
                    map.put(c.val, Query6fFindOptJoinTree.class);
                    break;
                case Q7:
                    map.put(c.val, Query7.class);
                    break;
                case Q7a:
                    map.put(c.val, Query7a.class);
                    break;
                case Q7aFindOptJoinTree:
                    map.put(c.val, Query7aFindOptJoinTree.class);
                    break;
                case Q7aOptJoinTreeOptOrdering:
                    map.put(c.val, Query7aOptJoinTreeOptOrdering.class);
                    break;
                case Q7b:
                    map.put(c.val, Query7b.class);
                    break;
                case Q7bFindOptJoinTree:
                    map.put(c.val, Query7bFindOptJoinTree.class);
                    break;
                case Q7c:
                    map.put(c.val, Query7c.class);
                    break;
                case Q7cFindOptJoinTree:
                    map.put(c.val, Query7cFindOptJoinTree.class);
                    break;
                case Q8:
                    map.put(c.val, Query84.class);
                    break;
                case Q8a:
                    map.put(c.val, Query8a.class);
                    break;
                case Q8aFindOptJoinTree:
                    map.put(c.val, Query8aFindOptJoinTree.class);
                    break;
                case Q8b:
                    map.put(c.val, Query8b.class);
                    break;
                case Q8bFindOptJoinTree:
                    map.put(c.val, Query8bFindOptJoinTree.class);
                    break;
                case Q8c:
                    map.put(c.val, Query8c.class);
                    break;
                case Q8cFindOptJoinTree:
                    map.put(c.val, Query8cFindOptJoinTree.class);
                    break;
                case Q8cOptJoinTreeOptOrdering:
                    map.put(c.val, Query8cOptJoinTreeOptOrdering.class);
                    break;
                case Q8d:
                    map.put(c.val, Query8d.class);
                    break;
                case Q8dFindOptJoinTree:
                    map.put(c.val, Query8dFindOptJoinTree.class);
                    break;
                case Q9:
                    map.put(c.val, Query9.class);
                    break;
                case Q9a:
                    map.put(c.val, Query9a.class);
                    break;
                case Q9aFindOptJoinTree:
                    map.put(c.val, Query9aFindOptJoinTree.class);
                    break;
                case Q9b:
                    map.put(c.val, Query9b.class);
                    break;
                case Q9bFindOptJoinTree:
                    map.put(c.val, Query9bFindOptJoinTree.class);
                    break;
                case Q9c:
                    map.put(c.val, Query9c.class);
                    break;
                case Q9cFindOptJoinTree:
                    map.put(c.val, Query9cFindOptJoinTree.class);
                    break;
                case Q9d:
                    map.put(c.val, Query9d.class);
                    break;
                case Q9dFindOptJoinTree:
                    map.put(c.val, Query9dFindOptJoinTree.class);
                    break;
                case Q10:
                    map.put(c.val, Query103.class);
                    break;
                case Q10a:
                    map.put(c.val, Query10a.class);
                    break;
                case Q10aFindOptJoinTree:
                    map.put(c.val, Query10aFindOptJoinTree.class);
                    break;
                case Q10b:
                    map.put(c.val, Query10b.class);
                    break;
                case Q10bFindOptJoinTree:
                    map.put(c.val, Query10bFindOptJoinTree.class);
                    break;
                case Q10c:
                    map.put(c.val, Query10c.class);
                    break;
                case Q10cFindOptJoinTree:
                    map.put(c.val, Query10cFindOptJoinTree.class);
                    break;
                case Q11:
                    map.put(c.val, Query11.class);
                    break;
                case Q11a:
                    map.put(c.val, Query11a.class);
                    break;
                case Q11aFindOptJoinTree:
                    map.put(c.val, Query11aFindOptJoinTree.class);
                    break;
                case Q11b:
                    map.put(c.val, Query11b.class);
                    break;
                case Q11bFindOptJoinTree:
                    map.put(c.val, Query11bFindOptJoinTree.class);
                    break;
                case Q11c:
                    map.put(c.val, Query11c.class);
                    break;
                case Q11cFindOptJoinTree:
                    map.put(c.val, Query11cFindOptJoinTree.class);
                    break;
                case Q11d:
                    map.put(c.val, Query11d.class);
                    break;
                case Q11dFindOptJoinTree:
                    map.put(c.val, Query11dFindOptJoinTree.class);
                    break;
                case Q12:
                    map.put(c.val, Query12.class);
                    break;
                case Q12a:
                    map.put(c.val, Query12a.class);
                    break;
                case Q12aFindOptJoinTree:
                    map.put(c.val, Query12aFindOptJoinTree.class);
                    break;
                case Q12b:
                    map.put(c.val, Query12b.class);
                    break;
                case Q12bFindOptJoinTree:
                    map.put(c.val, Query12bFindOptJoinTree.class);
                    break;
                case Q12c:
                    map.put(c.val, Query12c.class);
                    break;
                case Q12cFindOptJoinTree:
                    map.put(c.val, Query12cFindOptJoinTree.class);
                    break;
                case Q13:
                    map.put(c.val, org.zhu45.treetracker.benchmark.job.q13.Query13.class);
                    break;
                case Q13a:
                    map.put(c.val, Query13a.class);
                    break;
                case Q13b:
                    map.put(c.val, Query13b.class);
                    break;
                case Q13c:
                    map.put(c.val, Query13c.class);
                    break;
                case Q13d:
                    map.put(c.val, Query13d.class);
                    break;
                case Q14:
                    map.put(c.val, Query146.class);
                    break;
                case Q14a:
                    map.put(c.val, Query14a.class);
                    break;
                case Q14b:
                    map.put(c.val, Query14b.class);
                    break;
                case Q14c:
                    map.put(c.val, Query14c.class);
                    break;
                case Q15:
                    map.put(c.val, Query153.class);
                    break;
                case Q15a:
                    map.put(c.val, Query15a.class);
                    break;
                case Q15b:
                    map.put(c.val, Query15b.class);
                    break;
                case Q15c:
                    map.put(c.val, Query15c.class);
                    break;
                case Q15d:
                    map.put(c.val, Query15d.class);
                    break;
                case Q16:
                    map.put(c.val, Query161.class);
                    break;
                case Q16a:
                    map.put(c.val, Query16a.class);
                    break;
                case Q16b:
                    map.put(c.val, Query16b.class);
                    break;
                case Q16c:
                    map.put(c.val, Query16c.class);
                    break;
                case Q16d:
                    map.put(c.val, Query16d.class);
                    break;
                case Q17:
                    map.put(c.val, Query172.class);
                    break;
                case Q17a:
                    map.put(c.val, Query17a.class);
                    break;
                case Q17b:
                    map.put(c.val, Query17b.class);
                    break;
                case Q17c:
                    map.put(c.val, Query17c.class);
                    break;
                case Q17d:
                    map.put(c.val, Query17d.class);
                    break;
                case Q17e:
                    map.put(c.val, Query17e.class);
                    break;
                case Q17f:
                    map.put(c.val, Query17f.class);
                    break;
                case Q18:
                    map.put(c.val, Query181.class);
                    break;
                case Q18a:
                    map.put(c.val, Query18a.class);
                    break;
                case Q18b:
                    map.put(c.val, Query18b.class);
                    break;
                case Q18c:
                    map.put(c.val, Query18c.class);
                    break;
                case Q19:
                    map.put(c.val, Query191.class);
                    break;
                case Q19a:
                    map.put(c.val, Query19a.class);
                    break;
                case Q19aEstimationTroubleShooting:
                    map.put(c.val, Query19aEstimationTroubleShooting.class);
                    break;
                case Q19b:
                    map.put(c.val, Query19b.class);
                    break;
                case Q19c:
                    map.put(c.val, Query19c.class);
                    break;
                case Q19d:
                    map.put(c.val, Query19d.class);
                    break;
                case Q20:
                    map.put(c.val, Query20.class);
                    break;
                case Q20a:
                    map.put(c.val, Query20a.class);
                    break;
                case Q20b:
                    map.put(c.val, Query20b.class);
                    break;
                case Q20c:
                    map.put(c.val, Query20c.class);
                    break;
                case Q21:
                    map.put(c.val, Query211.class);
                    break;
                case Q21a:
                    map.put(c.val, Query21a.class);
                    break;
                case Q21b:
                    map.put(c.val, Query21b.class);
                    break;
                case Q21c:
                    map.put(c.val, Query21c.class);
                    break;
                case Q22:
                    map.put(c.val, Query22.class);
                    break;
                case Q22a:
                    map.put(c.val, Query22a.class);
                    break;
                case Q22b:
                    map.put(c.val, Query22b.class);
                    break;
                case Q22c:
                    map.put(c.val, Query22c.class);
                    break;
                case Q22d:
                    map.put(c.val, Query22d.class);
                    break;
                case Q23:
                    map.put(c.val, Query23.class);
                    break;
                case Q23a:
                    map.put(c.val, Query23a.class);
                    break;
                case Q23b:
                    map.put(c.val, Query23b.class);
                    break;
                case Q23c:
                    map.put(c.val, Query23c.class);
                    break;
                case Q24a:
                    map.put(c.val, Query24a.class);
                    break;
                case Q24b:
                    map.put(c.val, Query24b.class);
                    break;
                case Q25c:
                    map.put(c.val, Query25c.class);
                    break;
                case Q25:
                    map.put(c.val, Query25.class);
                    break;
                case Q25a:
                    map.put(c.val, Query25a.class);
                    break;
                case Q25b:
                    map.put(c.val, Query25b.class);
                    break;
                case Q26:
                    map.put(c.val, Query26.class);
                    break;
                case Q26a:
                    map.put(c.val, Query26a.class);
                    break;
                case Q26b:
                    map.put(c.val, Query26b.class);
                    break;
                case Q26c:
                    map.put(c.val, Query26c.class);
                    break;
                case Q27:
                    map.put(c.val, Query273.class);
                    break;
                case Q27a:
                    map.put(c.val, Query27a.class);
                    break;
                case Q27b:
                    map.put(c.val, Query27b.class);
                    break;
                case Q27c:
                    map.put(c.val, Query27c.class);
                    break;
                case Q28:
                    map.put(c.val, Query28.class);
                    break;
                case Q28a:
                    map.put(c.val, Query28a.class);
                    break;
                case Q28b:
                    map.put(c.val, Query28b.class);
                    break;
                case Q28c:
                    map.put(c.val, Query28c.class);
                    break;
                case Q29a:
                    map.put(c.val, Query29a.class);
                    break;
                case Q29b:
                    map.put(c.val, Query29b.class);
                    break;
                case Q29c:
                    map.put(c.val, Query29c.class);
                    break;
                case Q30:
                    map.put(c.val, Query30.class);
                    break;
                case Q30a:
                    map.put(c.val, Query30a.class);
                    break;
                case Q30b:
                    map.put(c.val, Query30b.class);
                    break;
                case Q30c:
                    map.put(c.val, Query30c.class);
                    break;
                case Q31:
                    map.put(c.val, Query311.class);
                    break;
                case Q31a:
                    map.put(c.val, Query31a.class);
                    break;
                case Q31b:
                    map.put(c.val, Query31b.class);
                    break;
                case Q31c:
                    map.put(c.val, Query31c.class);
                    break;
                case Q32:
                    map.put(c.val, org.zhu45.treetracker.benchmark.job.q32.Query32.class);
                    break;
                case Q32a:
                    map.put(c.val, Query32a.class);
                    break;
                case Q32b:
                    map.put(c.val, Query32b.class);
                    break;
                case Q33:
                    map.put(c.val, Query331.class);
                    break;
                case Q33a:
                    map.put(c.val, Query33a.class);
                    break;
                case Q33b:
                    map.put(c.val, Query33b.class);
                    break;
                case Q33c:
                    map.put(c.val, Query33c.class);
                    break;
            }
        }
    }

    private final String val;

    private JOBQueries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
