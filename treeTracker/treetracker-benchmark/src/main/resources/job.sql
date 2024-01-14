-- JOB query written with our own setup

-- Q1a
/*+
Leading( ((((imdb_int.movie_info_idx imdb.q1a_info_type) imdb_int.title) imdb.q1a_company_type) imdb.q1a_movie_companies))
HashJoin (imdb_int.movie_info_idx imdb.q1a_info_type)
HashJoin (imdb_int.movie_info_idx imdb.q1a_info_type imdb_int.title)
HashJoin (imdb_int.movie_info_idx imdb.q1a_info_type imdb_int.title imdb.q1a_company_type)
HashJoin (imdb_int.movie_info_idx imdb.q1a_info_type imdb_int.title imdb.q1a_company_type imdb.q1a_movie_companies)
SeqScan(imdb_int.movie_info_idx)
SeqScan(imdb.q1a_info_type)
SeqScan(imdb_int.title)
SeqScan(imdb.q1a_company_type)
SeqScan(imdb.q1a_movie_companies)
*/
explain (FORMAT JSON, ANALYZE) SELECT *
FROM imdb.q1a_company_type
         natural join
     imdb.q1a_info_type
         natural join
     imdb.q1a_movie_companies
         natural join
     imdb_int.movie_info_idx
         natural join
     imdb_int.title;

-- Q1b
--- 3
SELECT count(*)
FROM imdb.company_type AS ct,
     imdb.info_type AS it,
     imdb.movie_companies AS mc,
     imdb.movie_info_idx AS mi_idx,
     imdb.title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'bottom 10 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND t.production_year BETWEEN 2005 AND 2010
  AND ct.company_type_id = mc.company_type_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.info_type_id = mi_idx.info_type_id;


select count(*)
from imdb.q1b_company_type
         natural join
     imdb.q1b_info_type
         natural join
     imdb.q1b_movie_companies
         natural join
     imdb.q1b_title
         natural join
     imdb_int.movie_info_idx;

-- Q2a
--- 7834
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cn.country_code ='[de]'
  AND k.keyword ='character-name-in-title'
  AND cn.company_id = mc.company_id
  AND mc.movie_id = t.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.keyword_id = k.keyword_id
  AND mc.movie_id = mk.movie_id;

SELECT count(*)
FROM imdb.q2a_company_name natural join
     imdb.q2a_keyword natural join
    imdb_int.movie_companies natural join
    imdb_int.movie_keyword natural join
    imdb_int.title;

-- Q3a
--- 206
SELECT count(*)
FROM imdb.keyword AS k,
     imdb.movie_info AS mi,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE k.keyword LIKE '%sequel%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND t.production_year > 2005
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.keyword_id = mk.keyword_id;

SELECT count(*)
FROM imdb.q3a_keyword natural join
    imdb.q3a_movie_info natural join
    imdb.q3a_title natural join
    imdb_int.movie_keyword;

-- Q4a
--- 740
SELECT count(*)
FROM imdb.info_type AS it,
     imdb.keyword AS k,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE it.info ='rating'
  AND k.keyword LIKE '%sequel%'
  AND mi_idx.info > '5.0'
  AND t.production_year > 2005
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it.info_type_id = mi_idx.info_type_id;

SELECT count(*)
FROM imdb.q4a_info_type natural join
    imdb.q4a_keyword natural join
    imdb.q4a_movie_info_idx natural join
    imdb.q4a_title natural join
    imdb_int.movie_keyword;


-- Q5b
--- 0
SELECT count(*)
FROM imdb.company_type AS ct,
     imdb.info_type AS it,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.title AS t
WHERE ct.kind = 'production companies'
  AND mc.note LIKE '%(VHS)%'
  AND mc.note LIKE '%(USA)%'
  AND mc.note LIKE '%(1994)%'
  AND mi.info IN ('USA',
                  'America')
  AND t.production_year > 2010
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mc.movie_id
  AND mc.movie_id = mi.movie_id
  AND ct.company_type_id = mc.company_type_id
  AND it.info_type_id = mi.info_type_id;

SELECT count(*)
FROM imdb.q5b_company_type natural join
    imdb.q5b_movie_companies natural join
    imdb.q5b_movie_info natural join
    imdb.q5b_title natural join
    imdb_int.info_type;

-- Q6a
--- 6
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.keyword AS k,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE k.keyword = 'marvel-cinematic-universe'
  AND n.name LIKE '%Downey%Robert%'
  AND t.production_year > 2010
  AND k.keyword_id = mk.keyword_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = ci.movie_id
  AND ci.movie_id = mk.movie_id
  AND n.person_id = ci.person_id;

SELECT count(*)
FROM imdb.q6a_keyword natural join
     imdb.q6a_name natural join
     imdb.q6a_title natural join
     imdb_int.cast_info natural join
     imdb_int.movie_keyword;
-- Q7a
--- 32
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.cast_info AS ci,
     imdb.info_type AS it,
     imdb.link_type AS lt,
     imdb.movie_link AS ml,
     imdb.name AS n,
     imdb.person_info AS pi,
     imdb.title AS t
WHERE an.name LIKE '%a%'
  AND it.info ='mini biography'
  AND lt.link ='features'
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender='m'
    OR (n.gender = 'f'
        AND n.name LIKE 'B%'))
  AND pi.note ='Volker Boehm'
  AND t.production_year BETWEEN 1980 AND 1995
  AND n.person_id = an.person_id
  AND n.person_id = pi.person_id
  AND ci.person_id = n.person_id
  AND t.movie_id = ci.movie_id
  AND ml.linked_movie_id = t.movie_id
  AND lt.link_type_id = ml.link_type_id
  AND it.info_type_id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id;

SELECT count(*)
FROM
imdb.q7a_aka_name natural join
    imdb.q7a_info_type natural join
    imdb.q7a_link_type natural join
    imdb.q7a_name natural join
    imdb.q7a_person_info natural join
    imdb.q7a_title natural join
    imdb_int.cast_info natural join
    imdb.q7a_movie_link;

-- Q7b
--- 16
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.cast_info AS ci,
     imdb.info_type AS it,
     imdb.link_type AS lt,
     imdb.movie_link AS ml,
     imdb.name AS n,
     imdb.person_info AS pi,
     imdb.title AS t
WHERE an.name LIKE '%a%'
  AND it.info ='mini biography'
  AND lt.link ='features'
  AND n.name_pcode_cf LIKE 'D%'
  AND n.gender='m'
  AND pi.note ='Volker Boehm'
  AND t.production_year BETWEEN 1980 AND 1984
  AND n.person_id = an.person_id
  AND n.person_id = pi.person_id
  AND ci.person_id = n.person_id
  AND t.movie_id = ci.movie_id
  AND ml.linked_movie_id = t.movie_id
  AND lt.link_type_id = ml.link_type_id
  AND it.info_type_id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id;

SELECT count(*)
FROM
    imdb.q7b_aka_name natural join
    imdb.q7b_info_type natural join
    imdb.q7b_link_type natural join
    imdb.q7b_name natural join
    imdb.q7b_person_info natural join
    imdb.q7b_title natural join
    imdb_int.cast_info natural join
    imdb.q7b_movie_link;

-- Q7c
--- 68185
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.cast_info AS ci,
     imdb.info_type AS it,
     imdb.link_type AS lt,
     imdb.movie_link AS ml,
     imdb.name AS n,
     imdb.person_info AS pi,
     imdb.title AS t
WHERE an.name IS NOT NULL
  AND (an.name LIKE '%a%'
    OR an.name LIKE 'A%')
  AND it.info ='mini biography'
  AND lt.link IN ('references',
                  'referenced in',
                  'features',
                  'featured in')
  AND n.name_pcode_cf BETWEEN 'A' AND 'F'
  AND (n.gender='m'
    OR (n.gender = 'f'
        AND n.name LIKE 'A%'))
  AND pi.note IS NOT NULL
  AND t.production_year BETWEEN 1980 AND 2010
  AND n.person_id = an.person_id
  AND n.person_id = pi.person_id
  AND ci.person_id = n.person_id
  AND t.movie_id = ci.movie_id
  AND ml.linked_movie_id = t.movie_id
  AND lt.link_type_id = ml.link_type_id
  AND it.info_type_id = pi.info_type_id
  AND pi.person_id = an.person_id
  AND pi.person_id = ci.person_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = ml.linked_movie_id;

SELECT count(*)
FROM
    imdb.q7c_aka_name natural join
    imdb.q7c_info_type natural join
    imdb.q7c_link_type natural join
    imdb.q7c_name natural join
    imdb.q7c_person_info natural join
    imdb.q7c_title natural join
    imdb_int.cast_info natural join
    imdb.q7c_movie_link;

-- Q8a
--- 62
SELECT count(*)
FROM imdb.aka_name AS an1,
     imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.movie_companies AS mc,
     imdb.name AS n1,
     imdb.role_type AS rt,
     imdb.title AS t
WHERE ci.note ='(voice: English version)'
  AND cn.country_code ='[jp]'
  AND mc.note LIKE '%(Japan)%'
  AND mc.note NOT LIKE '%(USA)%'
  AND n1.name LIKE '%Yo%'
  AND n1.name NOT LIKE '%Yu%'
  AND rt.role ='actress'
  AND an1.person_id = n1.person_id
  AND n1.person_id = ci.person_id
  AND ci.movie_id = t.movie_id
  AND t.movie_id = mc.movie_id
  AND mc.company_id = cn.company_id
  AND ci.role_id = rt.role_id
  AND an1.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id;

SELECT count(*)
FROM imdb.q8a_cast_info natural join
    imdb.q8a_company_name natural join
    imdb.q8a_movie_companies natural join
    imdb.q8a_name natural join
    imdb.q8a_role_type natural join
    imdb_int.aka_name natural join
    imdb_int.title;

-- Q9a
--- 121
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.char_name AS chn,
     imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.movie_companies AS mc,
     imdb.name AS n,
     imdb.role_type AS rt,
     imdb.title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND mc.note IS NOT NULL
  AND (mc.note LIKE '%(USA)%'
    OR mc.note LIKE '%(worldwide)%')
  AND n.gender ='f'
  AND n.name LIKE '%Ang%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2005 AND 2015
  AND ci.movie_id = t.movie_id
  AND t.movie_id = mc.movie_id
  AND ci.movie_id = mc.movie_id
  AND mc.company_id = cn.company_id
  AND ci.role_id = rt.role_id
  AND n.person_id = ci.person_id
  AND chn.person_role_id = ci.person_role_id
  AND an.person_id = n.person_id
  AND an.person_id = ci.person_id;

SELECT count(*)
FROM imdb.q9a_cast_info natural join
    imdb.q9a_company_name natural join
    imdb.q9a_movie_companies natural join
    imdb.q9a_name natural join
    imdb.q9a_role_type natural join
    imdb.q9a_title natural join
    imdb_int.aka_name natural join
    imdb_int.char_name;


-- Q10a
SELECT *
FROM imdb.q10a_cast_info natural join
    imdb.q10a_company_name natural join
    imdb.q10a_role_type natural join
    imdb.q10a_title natural join
    imdb_int.char_name natural join
    imdb_int.company_type natural join
    imdb_int.movie_companies;

-- Q10b
SELECT *
FROM imdb.q10b_cast_info natural join
     imdb.q10b_company_name natural join
     imdb.q10b_role_type natural join
     imdb.q10b_title natural join
    imdb_int.char_name natural join
     imdb_int.company_type natural join
     imdb_int.movie_companies;

-- Q10c
--- output: 10
SELECT count(*)
FROM imdb.char_name AS chn,
     imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.movie_companies AS mc,
     imdb.role_type AS rt,
     imdb.title AS t
WHERE ci.note LIKE '%(producer)%'
  AND cn.country_code = '[us]'
  AND t.production_year > 1990
  AND t.movie_id = mc.movie_id
  AND t.movie_id = ci.movie_id
  AND ci.movie_id = mc.movie_id
  AND chn.person_role_id = ci.person_role_id
  AND rt.role_id = ci.role_id
  AND cn.company_id = mc.company_id
  AND ct.company_type_id = mc.company_type_id;

SELECT count(*)
FROM imdb.q10c_cast_info natural join
     imdb.q10c_company_name natural join
     imdb.q10c_title natural join
     imdb_int.char_name natural join
     imdb_int.company_type natural join
     imdb_int.movie_companies natural join
    imdb_int.role_type;

-- Q11a
SELECT *
FROM imdb.q11a_company_type natural join
     imdb.q11a_company_name natural join
     imdb.q11a_keyword natural join
     imdb.q11a_link_type natural join
     imdb.q11a_movie_companies natural join
    imdb_int.movie_keyword natural join
    imdb_int.movie_link natural join
     imdb.q11a_title;

-- Q11b
SELECT *
FROM imdb.q11b_company_type natural join
     imdb.q11b_company_name natural join
     imdb.q11b_keyword natural join
     imdb.q11b_link_type natural join
     imdb.q11b_movie_companies natural join
     imdb_int.movie_keyword natural join
     imdb_int.movie_link natural join
     imdb.q11b_title;

-- Q11c
SELECT *
FROM imdb.q11c_company_type natural join
     imdb.q11c_company_name natural join
     imdb.q11c_keyword natural join
     imdb.link_type natural join
     imdb.q11c_movie_companies natural join
     imdb_int.movie_keyword natural join
     imdb_int.movie_link natural join
     imdb.q11c_title;

-- Q11d
--- 14899
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.keyword AS k,
     imdb.link_type AS lt,
     imdb.movie_companies AS mc,
     imdb.movie_keyword AS mk,
     imdb.movie_link AS ml,
     imdb.title AS t
WHERE cn.country_code !='[pl]'
  AND ct.kind != 'production companies'
  AND ct.kind IS NOT NULL
  AND k.keyword IN ('sequel',
                    'revenge',
                    'based-on-novel')
  AND mc.note IS NOT NULL
  AND t.production_year > 1950
  AND lt.link_type_id = ml.link_type_id
  AND ml.movie_id = t.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.keyword_id = k.keyword_id
  AND t.movie_id = mc.movie_id
  AND mc.company_type_id = ct.company_type_id
  AND mc.company_id = cn.company_id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id;


SELECT count(*)
FROM imdb.q11d_company_type natural join
     imdb.q11d_company_name natural join
     imdb.q11d_keyword natural join
     imdb.link_type natural join
     imdb.q11d_movie_companies natural join
     imdb_int.movie_keyword natural join
     imdb_int.movie_link natural join
     imdb.q11d_title;

-- Q12a
--- 397
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.title AS t
WHERE cn.country_code = '[us]'
  AND ct.kind = 'production companies'
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama',
                  'Horror')
  AND mi_idx.info > '8.0'
  AND t.production_year BETWEEN 2005 AND 2008
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mi.info_type_id = it1.info_type_id
  AND mi_idx.info_type_id = it2.info_type_id
  AND t.movie_id = mc.movie_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;


SELECT count(*)
FROM imdb.q12a_company_type natural join
    imdb.q12a_company_name natural join
    imdb.q12a_info_type1 natural join
    imdb.q12a_info_type2 natural join
    imdb.q12a_movie_info natural join
    imdb.q12a_movie_info_idx2 natural join
    imdb.q12a_title natural join
    imdb_int.movie_companies;

-- Q12b
--- 10
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind IS NOT NULL
  AND (ct.kind ='production companies'
    OR ct.kind = 'distributors')
  AND it1.info ='budget'
  AND it2.info ='bottom 10 rank'
  AND t.production_year >2000
  AND (t.title LIKE 'Birdemic%'
    OR t.title LIKE '%Movie%')
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mi.info_type_id = it1.info_type_id
  AND mi_idx.info_type_id = it2.info_type_id
  AND t.movie_id = mc.movie_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

SELECT count(*)
FROM imdb.q12b_company_type natural join
     imdb.q12b_company_name natural join
     imdb.q12b_info_type1 natural join
     imdb.q12b_info_type2 natural join
     imdb_int.movie_info natural join
     imdb.q12b_movie_info_idx2 natural join
     imdb.q12b_title natural join
     imdb_int.movie_companies;

-- Q12c
--- 4711
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.title AS t
WHERE cn.country_code = '[us]'
  AND ct.kind = 'production companies'
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Drama',
                  'Horror',
                  'Western',
                  'Family')
  AND mi_idx.info > '7.0'
  AND t.production_year BETWEEN 2000 AND 2010
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mi.info_type_id = it1.info_type_id
  AND mi_idx.info_type_id = it2.info_type_id
  AND t.movie_id = mc.movie_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id;

SELECT count(*)
FROM imdb.q12c_company_type natural join
     imdb.q12c_company_name natural join
     imdb.q12c_info_type1 natural join
     imdb.q12c_info_type2 natural join
     imdb.q12c_movie_info natural join
     imdb.q12c_movie_info_idx2 natural join
     imdb.q12c_title natural join
     imdb_int.movie_companies;

-- Q13a
--- 111101
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it,
     imdb.info_type AS it2,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS miidx,
     imdb.title AS t
WHERE cn.country_code ='[de]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND mi.movie_id = t.movie_id
  AND it2.info_type_id = mi.info_type_id
  AND kt.kind_id = t.kind_id
  AND mc.movie_id = t.movie_id
  AND cn.company_id = mc.company_id
  AND ct.company_type_id = mc.company_type_id
  AND miidx.movie_id = t.movie_id
  AND it.info_type_id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

SELECT count(*)
FROM imdb.q13a_company_type natural join
     imdb.q13a_company_name natural join
     imdb.q13a_info_type1 natural join
     imdb.q13a_info_type2 natural join
     imdb.q13a_kind_type natural join
    imdb_int.movie_companies natural join
     imdb.q13a_movie_info natural join
    imdb_int.movie_info_idx natural join
    imdb_int.title;

-- Q13b
--- 372
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it,
     imdb.info_type AS it2,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS miidx,
     imdb.title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND t.title != ''
  AND (t.title LIKE '%Champion%'
    OR t.title LIKE '%Loser%')
  AND mi.movie_id = t.movie_id
  AND it2.info_type_id = mi.info_type_id
  AND kt.kind_id = t.kind_id
  AND mc.movie_id = t.movie_id
  AND cn.company_id = mc.company_id
  AND ct.company_type_id = mc.company_type_id
  AND miidx.movie_id = t.movie_id
  AND it.info_type_id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

SELECT count(*)
FROM imdb.q13b_company_type natural join
     imdb.q13b_company_name natural join
     imdb.q13b_info_type1 natural join
     imdb.q13b_info_type2 natural join
     imdb.q13b_kind_type natural join
     imdb_int.movie_companies natural join
     imdb.q13b_movie_info natural join
     imdb_int.movie_info_idx natural join
     imdb.q13b_title;

-- Q13c
--- 53
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it,
     imdb.info_type AS it2,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS miidx,
     imdb.title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND t.title != ''
  AND (t.title LIKE 'Champion%'
    OR t.title LIKE 'Loser%')
  AND mi.movie_id = t.movie_id
  AND it2.info_type_id = mi.info_type_id
  AND kt.kind_id = t.kind_id
  AND mc.movie_id = t.movie_id
  AND cn.company_id = mc.company_id
  AND ct.company_type_id = mc.company_type_id
  AND miidx.movie_id = t.movie_id
  AND it.info_type_id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

SELECT count(*)
FROM imdb.q13c_company_type natural join
     imdb.q13c_company_name natural join
     imdb.q13c_info_type1 natural join
     imdb.q13c_info_type2 natural join
     imdb.q13c_kind_type natural join
     imdb_int.movie_companies natural join
     imdb.q13c_movie_info natural join
     imdb_int.movie_info_idx natural join
     imdb.q13c_title;

-- Q13d
--- 670390
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it,
     imdb.info_type AS it2,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS miidx,
     imdb.title AS t
WHERE cn.country_code ='[us]'
  AND ct.kind ='production companies'
  AND it.info ='rating'
  AND it2.info ='release dates'
  AND kt.kind ='movie'
  AND mi.movie_id = t.movie_id
  AND it2.info_type_id = mi.info_type_id
  AND kt.kind_id = t.kind_id
  AND mc.movie_id = t.movie_id
  AND cn.company_id = mc.company_id
  AND ct.company_type_id = mc.company_type_id
  AND miidx.movie_id = t.movie_id
  AND it.info_type_id = miidx.info_type_id
  AND mi.movie_id = miidx.movie_id
  AND mi.movie_id = mc.movie_id
  AND miidx.movie_id = mc.movie_id;

SELECT count(*)
FROM imdb.q13d_company_type natural join
     imdb.q13d_company_name natural join
     imdb.q13d_info_type1 natural join
     imdb.q13d_info_type2 natural join
     imdb.q13d_kind_type natural join
     imdb_int.movie_companies natural join
     imdb.q13d_movie_info natural join
     imdb_int.movie_info_idx natural join
     imdb_int.title;

-- Q14a
SELECT count(*)
FROM imdb.q14a_info_type1 natural join
     imdb.q14a_info_type2 natural join
     imdb.q14a_keyword natural join
     imdb.q14a_kind_type natural join
     imdb.q14a_movie_info natural join
     imdb.q14a_movie_info_idx2 natural join
     imdb.q14a_title natural join
    imdb_int.movie_keyword;
--- 761
SELECT count(*)
FROM imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind = 'movie'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2010
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id;

-- Q14b
--- 1
SELECT count(*)
FROM imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title')
  AND kt.kind = 'movie'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info > '6.0'
  AND t.production_year > 2010
  AND (t.title LIKE '%murder%'
    OR t.title LIKE '%Murder%'
    OR t.title LIKE '%Mord%')
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id;

SELECT count(*)
FROM
imdb.q14b_info_type1 natural join
imdb.q14b_info_type2 natural join
imdb.q14b_keyword natural join
imdb.q14b_kind_type natural join
imdb.q14b_movie_info natural join
imdb.q14b_movie_info_idx natural join
imdb.q14b_title natural join
imdb_int.movie_keyword;

-- Q14c
--- 4115
SELECT count(*)
FROM imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IS NOT NULL
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id;

SELECT count(*)
FROM
    imdb.q14c_info_type1 natural join
    imdb.q14c_info_type2 natural join
    imdb.q14c_keyword natural join
    imdb.q14c_kind_type natural join
    imdb.q14c_movie_info natural join
    imdb.q14c_movie_info_idx2 natural join
    imdb.q14c_title natural join
    imdb_int.movie_keyword;

-- Q15a
--- 328
SELECT count(*)
FROM imdb.aka_title AS at,
     imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND mc.note LIKE '%(200%)%'
  AND mc.note LIKE '%(worldwide)%'
  AND mi.note LIKE '%internet%'
  AND mi.info LIKE 'USA:% 200%'
  AND t.production_year > 2000
  AND t.movie_id = at.movie_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = at.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = at.movie_id
  AND mc.movie_id = at.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND cn.company_id = mc.company_id
  AND ct.company_type_id = mc.company_type_id;


SELECT count(*)
FROM imdb.q15a_company_name natural join
     imdb.q15a_info_type natural join
     imdb.q15a_movie_companies natural join
     imdb.q15a_movie_info natural join
     imdb.q15a_title natural join
    imdb_int.aka_title natural join
    imdb_int.company_type natural join
    imdb_int.keyword natural join
    imdb_int.movie_keyword;

-- Q16a
--- 385
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE cn.country_code ='[us]'
  AND k.keyword ='character-name-in-title'
  AND t.episode_nr >= 50
  AND t.episode_nr < 100
  AND an.person_id = n.person_id
  AND n.person_id = ci.person_id
  AND ci.movie_id = t.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.keyword_id = k.keyword_id
  AND t.movie_id = mc.movie_id
  AND mc.company_id = cn.company_id
  AND an.person_id = ci.person_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;


SELECT count(*)
FROM imdb.q16a_company_name natural join
     imdb.q16a_keyword natural join
     imdb.q16a_title natural join
    imdb_int.aka_name natural join
    imdb_int.cast_info natural join
    imdb_int.movie_companies natural join
    imdb_int.movie_keyword natural join
    imdb_int.name;

-- Q17a
SELECT *
FROM imdb.q17a_company_name natural join
     imdb.q17a_keyword natural join
     imdb.q17a_name natural join
    imdb_int.cast_info natural join
    imdb_int.movie_companies natural join
    imdb_int.movie_keyword natural join
    imdb_int.title;

-- Q17b
--- 52306
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE k.keyword ='character-name-in-title'
  AND n.name LIKE 'Z%'
  AND n.person_id = ci.person_id
  AND ci.movie_id = t.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.keyword_id = k.keyword_id
  AND t.movie_id = mc.movie_id
  AND mc.company_id = cn.company_id
  AND ci.movie_id = mc.movie_id
  AND ci.movie_id = mk.movie_id
  AND mc.movie_id = mk.movie_id;

SELECT count(*)
FROM imdb_int.company_name natural join
     imdb.q17b_keyword natural join
     imdb.q17b_name natural join
     imdb_int.cast_info natural join
     imdb_int.movie_companies natural join
     imdb_int.movie_keyword natural join
     imdb_int.title;


-- Q18a
--- 410
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(producer)',
                  '(executive producer)')
  AND it1.info = 'budget'
  AND it2.info = 'votes'
  AND n.gender = 'm'
  AND n.name LIKE '%Tim%'
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id;

SELECT count(*)
FROM imdb.q18a_cast_info natural join
     imdb.q18a_info_type1 natural join
     imdb.q18a_info_type2 natural join
     imdb.q18a_name natural join
    imdb_int.movie_info natural join
    imdb.q18a_movie_info_idx2 natural join
    imdb_int.title;

-- Q18b
--- 11
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Horror',
                  'Thriller')
  AND mi.note IS NULL
  AND mi_idx.info > '8.0'
  AND n.gender IS NOT NULL
  AND n.gender = 'f'
  AND t.production_year BETWEEN 2008 AND 2014
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id;

SELECT count(*)
FROM imdb.q18b_cast_info natural join
     imdb.q18b_info_type1 natural join
     imdb.q18b_info_type2 natural join
     imdb.q18b_name natural join
     imdb.q18b_movie_info natural join
     imdb.q18b_movie_info_idx2 natural join
     imdb.q18b_title;

-- Q18c
--- 28073
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND n.gender = 'm'
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id;

SELECT count(*)
FROM imdb.q18c_cast_info natural join
     imdb.q18c_info_type1 natural join
     imdb.q18c_info_type2 natural join
     imdb.q18c_name natural join
     imdb.q18c_movie_info natural join
     imdb.q18c_movie_info_idx2 natural join
     imdb_int.title;

-- Q19a
--- 184
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.char_name AS chn,
     imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.info_type AS it,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.name AS n,
     imdb.role_type AS rt,
     imdb.title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND mc.note IS NOT NULL
  AND (mc.note LIKE '%(USA)%'
    OR mc.note LIKE '%(worldwide)%')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
    OR mi.info LIKE 'USA:%200%')
  AND n.gender ='f'
  AND n.name LIKE '%Ang%'
  AND rt.role ='actress'
  AND t.production_year BETWEEN 2005 AND 2009
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = ci.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mi.movie_id = ci.movie_id
  AND cn.company_id = mc.company_id
  AND it.info_type_id = mi.info_type_id
  AND n.person_id = ci.person_id
  AND rt.role_id = ci.role_id
  AND n.person_id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.person_role_id = ci.person_role_id;

SELECT count(*)
FROM imdb.q19a_cast_info natural join
     imdb.q19a_company_name natural join
     imdb.q19a_info_type natural join
     imdb.q19a_movie_companies natural join
     imdb.q19a_movie_info natural join
     imdb.q19a_name natural join
     imdb.q19a_role_type natural join
     imdb.q19a_title natural join
    imdb_int.aka_name natural join
    imdb_int.char_name;

-- Q20a
--- 33
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.char_name AS chn,
     imdb.cast_info AS ci,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name NOT LIKE '%Sherlock%'
  AND (chn.name LIKE '%Tony%Stark%'
    OR chn.name LIKE '%Iron%Man%')
  AND k.keyword IN ('superhero',
                    'sequel',
                    'second-part',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence')
  AND kt.kind = 'movie'
  AND t.production_year > 1950
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = cc.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND ci.movie_id = cc.movie_id
  AND chn.person_role_id = ci.person_role_id
  AND n.person_id = ci.person_id
  AND k.keyword_id = mk.keyword_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM imdb.q20a_comp_cast_type1 natural join
     imdb.q20a_comp_cast_type2 natural join
     imdb.q20a_char_name natural join
     imdb.q20a_keyword natural join
     imdb.q20a_kind_type natural join
     imdb.q20a_title natural join
    imdb_int.complete_cast natural join
    imdb_int.cast_info natural join
    imdb_int.movie_keyword natural join
    imdb_int.name;

-- Q21a
--- 1410
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.keyword AS k,
     imdb.link_type AS lt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_keyword AS mk,
     imdb.movie_link AS ml,
     imdb.title AS t
WHERE cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
    OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Denish',
                  'Norwegian',
                  'German')
  AND t.production_year BETWEEN 1950 AND 2000
  AND lt.link_type_id = ml.link_type_id
  AND ml.movie_id = t.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.keyword_id = k.keyword_id
  AND t.movie_id = mc.movie_id
  AND mc.company_type_id = ct.company_type_id
  AND mc.company_id = cn.company_id
  AND mi.movie_id = t.movie_id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id;

SELECT count(*)
FROM imdb.q21a_company_name natural join
    imdb.q21a_company_type natural join
    imdb.q21a_keyword natural join
    imdb.q21a_link_type natural join
    imdb.q21a_movie_companies natural join
    imdb.q21a_movie_info natural join
    imdb.q21a_title natural join
    imdb_int.movie_keyword natural join
    imdb_int.movie_link;


-- Q22a
--- 2851
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Germany',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '7.0'
  AND t.production_year > 2008
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id;

SELECT count(*)
FROM imdb.q22a_company_name natural join
     imdb.q22a_info_type1 natural join
     imdb.q22a_info_type2 natural join
     imdb.q22a_keyword natural join
     imdb.q22a_kind_type natural join
     imdb.q22a_movie_companies natural join
     imdb.q22a_movie_info natural join
     imdb.q22a_movie_info_idx2 natural join
     imdb.q22a_title natural join
     imdb_int.company_type natural join
     imdb_int.movie_keyword;

-- Q22b
--- 31
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Germany',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '7.0'
  AND t.production_year > 2009
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id;

SELECT count(*)
FROM imdb.q22b_company_name natural join
 imdb.q22b_info_type1 natural join
 imdb.q22b_info_type2 natural join
 imdb.q22b_keyword natural join
 imdb.q22b_kind_type natural join
 imdb.q22b_movie_companies natural join
 imdb.q22b_movie_info natural join
 imdb.q22b_movie_info_idx2 natural join
 imdb.q22b_title natural join
 imdb_int.company_type natural join
 imdb_int.movie_keyword;

-- Q22c
--- 21489
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id;

SELECT count(*)
FROM imdb.q22c_company_name natural join
     imdb.q22c_info_type1 natural join
     imdb.q22c_info_type2 natural join
     imdb.q22c_keyword natural join
     imdb.q22c_kind_type natural join
     imdb.q22c_movie_companies natural join
     imdb.q22c_movie_info natural join
     imdb.q22c_movie_info_idx2 natural join
     imdb.q22c_title natural join
     imdb_int.company_type natural join
     imdb_int.movie_keyword;

-- Q22d
--- 46281
SELECT count(*)
FROM imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id;

SELECT count(*)
FROM imdb.q22d_company_name natural join
     imdb.q22d_info_type1 natural join
     imdb.q22d_info_type2 natural join
     imdb.q22d_keyword natural join
     imdb.q22d_kind_type natural join
     imdb_int.movie_companies natural join
     imdb.q22d_movie_info natural join
     imdb.q22d_movie_info_idx2 natural join
     imdb.q22d_title natural join
     imdb_int.company_type natural join
     imdb_int.movie_keyword;

-- Q23a
--- 618
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cct1.kind = 'complete+verified'
  AND cn.country_code = '[us]'
  AND it1.info = 'release dates'
  AND kt.kind IN ('movie')
  AND mi.note LIKE '%internet%'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'USA:% 199%'
    OR mi.info LIKE 'USA:% 200%')
  AND t.production_year > 2000
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND cn.company_id = mc.company_id
  AND ct.company_type_id = mc.company_type_id
  AND cct1.subject_id = cc.status_id;

SELECT count(*)
FROM imdb.q23a_comp_cast_type natural join
    imdb.q23a_company_name natural join
    imdb.q23a_info_type natural join
    imdb.q23a_kind_type natural join
    imdb.q23a_movie_info natural join
    imdb.q23a_title natural join
    imdb_int.complete_cast natural join
    imdb_int.company_type natural join
    imdb_int.keyword natural join
    imdb_int.movie_companies natural join
    imdb_int.movie_keyword;

-- Q24a
--- 275
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.char_name AS chn,
     imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.info_type AS it,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.role_type AS rt,
     imdb.title AS t
WHERE ci.note IN ('(voice)',
                  '(voice: Japanese version)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND k.keyword IN ('hero',
                    'martial-arts',
                    'hand-to-hand-combat')
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%201%'
    OR mi.info LIKE 'USA:%201%')
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.production_year > 2010
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mi.movie_id = ci.movie_id
  AND mi.movie_id = mk.movie_id
  AND ci.movie_id = mk.movie_id
  AND cn.company_id = mc.company_id
  AND it.info_type_id = mi.info_type_id
  AND n.person_id = ci.person_id
  AND rt.role_id = ci.role_id
  AND n.person_id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.person_role_id = ci.person_role_id
  AND k.keyword_id = mk.keyword_id;

SELECT count(*)
FROM imdb.q24a_cast_info natural join
    imdb.q24a_company_name natural join
    imdb.q24a_info_type natural join
    imdb.q24a_keyword natural join
    imdb.q24a_movie_info natural join
    imdb.q24a_name natural join
    imdb.q24a_role_type natural join
    imdb.q24a_title natural join
    imdb_int.aka_name natural join
    imdb_int.char_name natural join
    imdb_int.movie_companies natural join
    imdb_int.movie_keyword;


-- Q25a
--- 4407
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity')
  AND mi.info = 'Horror'
  AND n.gender = 'm'
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id;

SELECT count(*)
FROM
imdb.q25a_cast_info natural join
    imdb.q25a_info_type1 natural join
    imdb.q25a_info_type2 natural join
    imdb.q25a_keyword natural join
    imdb.q25a_movie_info natural join
    imdb.q25a_name natural join
    imdb.q25a_movie_info_idx2 natural join
    imdb_int.movie_keyword natural join
    imdb_int.title;

-- Q25b
--- 6
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity')
  AND mi.info = 'Horror'
  AND n.gender = 'm'
  AND t.production_year > 2010
  AND t.title LIKE 'Vampire%'
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id;

SELECT count(*)
FROM
    imdb.q25b_cast_info natural join
    imdb.q25b_info_type1 natural join
    imdb.q25b_info_type2 natural join
    imdb.q25b_keyword natural join
    imdb.q25b_movie_info natural join
    imdb.q25b_name natural join
    imdb.q25b_movie_info_idx2 natural join
    imdb_int.movie_keyword natural join
    imdb.q25b_title;

-- Q25c
--- 26153
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND n.gender = 'm'
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id;

SELECT count(*)
FROM
    imdb.q25c_cast_info natural join
    imdb.q25c_info_type1 natural join
    imdb.q25c_info_type2 natural join
    imdb.q25c_keyword natural join
    imdb.q25c_movie_info natural join
    imdb.q25c_name natural join
    imdb.q25c_movie_info_idx2 natural join
    imdb_int.movie_keyword natural join
    imdb_int.title;


-- Q26a
--- 1728
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.char_name AS chn,
     imdb.cast_info AS ci,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind LIKE '%complete%'
  AND chn.name IS NOT NULL
  AND (chn.name LIKE '%man%'
    OR chn.name LIKE '%Man%')
  AND it2.info = 'rating'
  AND k.keyword IN ('superhero',
                    'marvel-comics',
                    'based-on-comic',
                    'tv-special',
                    'fight',
                    'violence',
                    'magnet',
                    'web',
                    'claw',
                    'laser')
  AND kt.kind = 'movie'
  AND mi_idx.info > '7.0'
  AND t.production_year > 2000
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = cc.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND mk.movie_id = ci.movie_id
  AND mk.movie_id = cc.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND ci.movie_id = cc.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND cc.movie_id = mi_idx.movie_id
  AND chn.person_role_id = ci.person_role_id
  AND n.person_id = ci.person_id
  AND k.keyword_id = mk.keyword_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id
  AND it2.info_type_id = mi_idx.info_type_id;

SELECT count(*)
FROM imdb.q26a_comp_cast_type1 natural join
    imdb.q26a_comp_cast_type2 natural join
    imdb.q26a_char_name natural join
    imdb.q26a_info_type natural join
    imdb.q26a_keyword natural join
    imdb.q26a_kind_type natural join
    imdb.q26a_movie_info_idx natural join
    imdb.q26a_title natural join
    imdb_int.complete_cast natural join
    imdb_int.cast_info natural join
    imdb_int.movie_keyword natural join
    imdb_int.name;

-- Q27a
--- 477
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.keyword AS k,
     imdb.link_type AS lt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_keyword AS mk,
     imdb.movie_link AS ml,
     imdb.title AS t
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind = 'complete'
  AND cn.country_code !='[pl]'
  AND (cn.name LIKE '%Film%'
    OR cn.name LIKE '%Warner%')
  AND ct.kind ='production companies'
  AND k.keyword ='sequel'
  AND lt.link LIKE '%follow%'
  AND mc.note IS NULL
  AND mi.info IN ('Sweden',
                  'Germany',
                  'Swedish',
                  'German')
  AND t.production_year BETWEEN 1950 AND 2000
  AND lt.link_type_id = ml.link_type_id
  AND ml.movie_id = t.movie_id
  AND t.movie_id = mk.movie_id
  AND mk.keyword_id = k.keyword_id
  AND t.movie_id = mc.movie_id
  AND mc.company_type_id = ct.company_type_id
  AND mc.company_id = cn.company_id
  AND mi.movie_id = t.movie_id
  AND t.movie_id = cc.movie_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id
  AND ml.movie_id = mk.movie_id
  AND ml.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND ml.movie_id = mi.movie_id
  AND mk.movie_id = mi.movie_id
  AND mc.movie_id = mi.movie_id
  AND ml.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = cc.movie_id;

SELECT count(*)
FROM
imdb_int.complete_cast natural join
imdb.q27a_comp_cast_type1 natural join
imdb.q27a_comp_cast_type2 natural join
imdb.q27a_company_name natural join
imdb.q27a_company_type natural join
imdb.q27a_keyword natural join
imdb.q27a_link_type natural join
imdb.q27a_movie_companies natural join
imdb.q27a_movie_info natural join
imdb.q27a_title natural join
imdb_int.movie_keyword natural join
imdb_int.movie_link;

-- Q28a
--- 4803
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cct1.kind = 'crew'
  AND cct2.kind != 'complete+verified'
  AND cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2000
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM
imdb.q28a_comp_cast_type1 natural join
imdb.q28a_comp_cast_type2 natural join
imdb.q28a_company_name natural join
imdb.q28a_info_type1 natural join
imdb.q28a_info_type2 natural join
imdb.q28a_keyword natural join
imdb.q28a_kind_type natural join
imdb.q28a_movie_companies natural join
imdb.q28a_movie_info natural join
imdb.q28a_movie_info_idx2 natural join
imdb.q28a_title natural join
imdb_int.complete_cast natural join
imdb_int.company_type natural join
imdb_int.movie_keyword;

-- Q28b
--- 148
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cct1.kind = 'crew'
  AND cct2.kind != 'complete+verified'
  AND cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Germany',
                  'Swedish',
                  'German')
  AND mi_idx.info > '6.5'
  AND t.production_year > 2005
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM
    imdb.q28b_comp_cast_type1 natural join
    imdb.q28b_comp_cast_type2 natural join
    imdb.q28b_company_name natural join
    imdb.q28b_info_type1 natural join
    imdb.q28b_info_type2 natural join
    imdb.q28b_keyword natural join
    imdb.q28b_kind_type natural join
    imdb.q28b_movie_companies natural join
    imdb.q28b_movie_info natural join
    imdb.q28b_movie_info_idx2 natural join
    imdb.q28b_title natural join
    imdb_int.complete_cast natural join
    imdb_int.company_type natural join
    imdb_int.movie_keyword;

-- Q28c
--- 8373
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.company_name AS cn,
     imdb.company_type AS ct,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.kind_type AS kt,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind = 'complete'
  AND cn.country_code != '[us]'
  AND it1.info = 'countries'
  AND it2.info = 'rating'
  AND k.keyword IN ('murder',
                    'murder-in-title',
                    'blood',
                    'violence')
  AND kt.kind IN ('movie',
                  'episode')
  AND mc.note NOT LIKE '%(USA)%'
  AND mc.note LIKE '%(200%)%'
  AND mi.info IN ('Sweden',
                  'Norway',
                  'Germany',
                  'Denmark',
                  'Swedish',
                  'Danish',
                  'Norwegian',
                  'German',
                  'USA',
                  'American')
  AND mi_idx.info < '8.5'
  AND t.production_year > 2005
  AND kt.kind_id = t.kind_id
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = cc.movie_id
  AND mk.movie_id = mi.movie_id
  AND mk.movie_id = mi_idx.movie_id
  AND mk.movie_id = mc.movie_id
  AND mk.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi.movie_id = cc.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND k.keyword_id = mk.keyword_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND ct.company_type_id = mc.company_type_id
  AND cn.company_id = mc.company_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM
    imdb.q28c_comp_cast_type1 natural join
    imdb.q28c_comp_cast_type2 natural join
    imdb.q28c_company_name natural join
    imdb.q28c_info_type1 natural join
    imdb.q28c_info_type2 natural join
    imdb.q28c_keyword natural join
    imdb.q28c_kind_type natural join
    imdb.q28c_movie_companies natural join
    imdb.q28c_movie_info natural join
    imdb.q28c_movie_info_idx2 natural join
    imdb.q28c_title natural join
    imdb_int.complete_cast natural join
    imdb_int.company_type natural join
    imdb_int.movie_keyword;

-- Q29a
--- 1620
SELECT count(*)
FROM imdb.aka_name AS an,
     imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.char_name AS chn,
     imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.info_type AS it,
     imdb.info_type AS it3,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.person_info AS pi,
     imdb.role_type AS rt,
     imdb.title AS t
WHERE cct1.kind ='cast'
  AND cct2.kind ='complete+verified'
  AND chn.name = 'Queen'
  AND ci.note IN ('(voice)',
                  '(voice) (uncredited)',
                  '(voice: English version)')
  AND cn.country_code ='[us]'
  AND it.info = 'release dates'
  AND it3.info = 'trivia'
  AND k.keyword = 'computer-animation'
  AND mi.info IS NOT NULL
  AND (mi.info LIKE 'Japan:%200%'
    OR mi.info LIKE 'USA:%200%')
  AND n.gender ='f'
  AND n.name LIKE '%An%'
  AND rt.role ='actress'
  AND t.title = 'Shrek 2'
  AND t.production_year BETWEEN 2000 AND 2010
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mc.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = cc.movie_id
  AND mc.movie_id = ci.movie_id
  AND mc.movie_id = mi.movie_id
  AND mc.movie_id = mk.movie_id
  AND mc.movie_id = cc.movie_id
  AND mi.movie_id = ci.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND cn.company_id = mc.company_id
  AND it.info_type_id = mi.info_type_id
  AND n.person_id = ci.person_id
  AND rt.role_id = ci.role_id
  AND n.person_id = an.person_id
  AND ci.person_id = an.person_id
  AND chn.person_role_id = ci.person_role_id
  AND n.person_id = pi.person_id
  AND ci.person_id = pi.person_id
  AND it3.info_type_id = pi.info_type_id
  AND k.keyword_id = mk.keyword_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM imdb.q29a_comp_cast_type1 natural join
     imdb.q29a_comp_cast_type2 natural join
     imdb.q29a_char_name natural join
    imdb.q29a_cast_info natural join
    imdb.q29a_company_name natural join
    imdb.q29a_info_type1 natural join
    imdb.q29a_info_type2 natural join
    imdb.q29a_keyword natural join
    imdb.q29a_movie_info natural join
    imdb.q29a_name natural join
    imdb.q29a_role_type natural join
    imdb.q29a_title natural join
    imdb.q29a_person_info natural join
    imdb_int.aka_name natural join
    imdb_int.complete_cast natural join
    imdb_int.movie_companies natural join
    imdb_int.movie_keyword;

-- Q30a
--- 757
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.production_year > 2000
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = cc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM
imdb.q30a_comp_cast_type1 natural join
imdb.q30a_comp_cast_type2 natural join
imdb.q30a_cast_info natural join
imdb.q30a_info_type1 natural join
imdb.q30a_info_type2 natural join
imdb.q30a_keyword natural join
imdb.q30a_movie_info natural join
imdb.q30a_name natural join
imdb.q30a_title natural join
imdb_int.complete_cast natural join
imdb.q30a_movie_info_idx2 natural join
imdb_int.movie_keyword;

-- Q30b
--- 28
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE cct1.kind IN ('cast',
                    'crew')
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.production_year > 2000
  AND (t.title LIKE '%Freddy%'
    OR t.title LIKE '%Jason%'
    OR t.title LIKE 'Saw%')
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = cc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM
    imdb.q30b_comp_cast_type1 natural join
    imdb.q30b_comp_cast_type2 natural join
    imdb.q30b_cast_info natural join
    imdb.q30b_info_type1 natural join
    imdb.q30b_info_type2 natural join
    imdb.q30b_keyword natural join
    imdb.q30b_movie_info natural join
    imdb.q30b_name natural join
    imdb.q30b_title natural join
    imdb_int.complete_cast natural join
    imdb.q30b_movie_info_idx2 natural join
    imdb_int.movie_keyword;

-- Q30c
--- 8024
SELECT count(*)
FROM imdb.complete_cast AS cc,
     imdb.comp_cast_type AS cct1,
     imdb.comp_cast_type AS cct2,
     imdb.cast_info AS ci,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE cct1.kind = 'cast'
  AND cct2.kind ='complete+verified'
  AND ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND n.gender = 'm'
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = cc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = cc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = cc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = cc.movie_id
  AND mk.movie_id = cc.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id
  AND cct1.subject_id = cc.subject_id
  AND cct2.subject_id = cc.status_id;

SELECT count(*)
FROM
    imdb.q30c_comp_cast_type1 natural join
    imdb.q30c_comp_cast_type2 natural join
    imdb.q30c_cast_info natural join
    imdb.q30c_info_type1 natural join
    imdb.q30c_info_type2 natural join
    imdb.q30c_keyword natural join
    imdb.q30c_movie_info natural join
    imdb.q30c_name natural join
    imdb_int.title natural join
    imdb_int.complete_cast natural join
    imdb.q30c_movie_info_idx2 natural join
    imdb_int.movie_keyword;


-- Q31a
--- 1273
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id
  AND cn.company_id = mc.company_id;

SELECT count(*)
FROM
    imdb.q31a_cast_info natural join
    imdb.q31a_company_name natural join
    imdb.q31a_info_type1 natural join
    imdb.q31a_info_type2 natural join
    imdb.q31a_keyword natural join
    imdb.q31a_movie_info natural join
    imdb.q31a_name natural join
    imdb_int.movie_companies natural join
    imdb.q31a_movie_info_idx2 natural join
    imdb_int.movie_keyword natural join
    imdb_int.title;

-- Q31b
--- 84
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mc.note LIKE '%(Blu-ray)%'
  AND mi.info IN ('Horror',
                  'Thriller')
  AND n.gender = 'm'
  AND t.production_year > 2000
  AND (t.title LIKE '%Freddy%'
    OR t.title LIKE '%Jason%'
    OR t.title LIKE 'Saw%')
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id
  AND cn.company_id = mc.company_id;

SELECT count(*)
FROM
    imdb.q31b_cast_info natural join
    imdb.q31b_company_name natural join
    imdb.q31b_info_type1 natural join
    imdb.q31b_info_type2 natural join
    imdb.q31b_keyword natural join
    imdb.q31b_movie_info natural join
    imdb.q31b_name natural join
    imdb.q31b_movie_companies natural join
    imdb.q31b_movie_info_idx2 natural join
    imdb_int.movie_keyword natural join
    imdb.q31b_title;

-- Q31c
--- 2825
SELECT count(*)
FROM imdb.cast_info AS ci,
     imdb.company_name AS cn,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.keyword AS k,
     imdb.movie_companies AS mc,
     imdb.movie_info AS mi,
     imdb.movie_info_idx AS mi_idx,
     imdb.movie_keyword AS mk,
     imdb.name AS n,
     imdb.title AS t
WHERE ci.note IN ('(writer)',
                  '(head writer)',
                  '(written by)',
                  '(story)',
                  '(story editor)')
  AND cn.name LIKE 'Lionsgate%'
  AND it1.info = 'genres'
  AND it2.info = 'votes'
  AND k.keyword IN ('murder',
                    'violence',
                    'blood',
                    'gore',
                    'death',
                    'female-nudity',
                    'hospital')
  AND mi.info IN ('Horror',
                  'Action',
                  'Sci-Fi',
                  'Thriller',
                  'Crime',
                  'War')
  AND t.movie_id = mi.movie_id
  AND t.movie_id = mi_idx.movie_id
  AND t.movie_id = ci.movie_id
  AND t.movie_id = mk.movie_id
  AND t.movie_id = mc.movie_id
  AND ci.movie_id = mi.movie_id
  AND ci.movie_id = mi_idx.movie_id
  AND ci.movie_id = mk.movie_id
  AND ci.movie_id = mc.movie_id
  AND mi.movie_id = mi_idx.movie_id
  AND mi.movie_id = mk.movie_id
  AND mi.movie_id = mc.movie_id
  AND mi_idx.movie_id = mk.movie_id
  AND mi_idx.movie_id = mc.movie_id
  AND mk.movie_id = mc.movie_id
  AND n.person_id = ci.person_id
  AND it1.info_type_id = mi.info_type_id
  AND it2.info_type_id = mi_idx.info_type_id
  AND k.keyword_id = mk.keyword_id
  AND cn.company_id = mc.company_id;

SELECT count(*)
FROM
    imdb.q31c_cast_info natural join
    imdb.q31c_company_name natural join
    imdb.q31c_info_type1 natural join
    imdb.q31c_info_type2 natural join
    imdb.q31c_keyword natural join
    imdb.q31c_movie_info natural join
    imdb_int.name natural join
    imdb_int.movie_companies natural join
    imdb.q31c_movie_info_idx2 natural join
    imdb_int.movie_keyword natural join
    imdb_int.title;

-- Q32b
--- 4388
SELECT count(*)
FROM imdb.keyword AS k,
     imdb.link_type AS lt,
     imdb.movie_keyword AS mk,
     imdb.movie_link AS ml,
     imdb.title AS t1,
     imdb.title AS t2
WHERE k.keyword ='character-name-in-title'
  AND mk.keyword_id = k.keyword_id
  AND t1.movie_id = mk.movie_id
  AND ml.movie_id = t1.movie_id
  AND ml.linked_movie_id = t2.movie_id
  AND lt.link_type_id = ml.link_type_id
  AND mk.movie_id = t1.movie_id;

SELECT count(*)
FROM imdb.q32b_keyword natural join
     imdb.q32b_title1 natural join
     imdb.q32b_title2 natural join
     imdb_int.link_type natural join
     imdb_int.movie_keyword natural join
     imdb_int.movie_link;


-- Q33c
--- 114
SELECT count(*)
FROM imdb.company_name AS cn1,
     imdb.company_name AS cn2,
     imdb.info_type AS it1,
     imdb.info_type AS it2,
     imdb.kind_type AS kt1,
     imdb.kind_type AS kt2,
     imdb.link_type AS lt,
     imdb.movie_companies AS mc1,
     imdb.movie_companies AS mc2,
     imdb.movie_info_idx AS mi_idx1,
     imdb.movie_info_idx AS mi_idx2,
     imdb.movie_link AS ml,
     imdb.title AS t1,
     imdb.title AS t2
WHERE cn1.country_code != '[us]'
  AND it1.info = 'rating'
  AND it2.info = 'rating'
  AND kt1.kind IN ('tv series',
                   'episode')
  AND kt2.kind IN ('tv series',
                   'episode')
  AND lt.link IN ('sequel',
                  'follows',
                  'followed by')
  AND mi_idx2.info < '3.5'
  AND t2.production_year BETWEEN 2000 AND 2010
  AND lt.link_type_id = ml.link_type_id
  AND t1.movie_id = ml.movie_id
  AND t2.movie_id = ml.linked_movie_id
  AND it1.info_type_id = mi_idx1.info_type_id
  AND t1.movie_id = mi_idx1.movie_id
  AND kt1.kind_id = t1.kind_id
  AND cn1.company_id = mc1.company_id
  AND t1.movie_id = mc1.movie_id
  AND ml.movie_id = mi_idx1.movie_id
  AND ml.movie_id = mc1.movie_id
  AND mi_idx1.movie_id = mc1.movie_id
  AND it2.info_type_id = mi_idx2.info_type_id
  AND t2.movie_id = mi_idx2.movie_id
  AND kt2.kind_id = t2.kind_id
  AND cn2.company_id = mc2.company_id
  AND t2.movie_id = mc2.movie_id
  AND ml.linked_movie_id = mi_idx2.movie_id
  AND ml.linked_movie_id = mc2.movie_id
  AND mi_idx2.movie_id = mc2.movie_id;

SELECT count(*)
FROM
imdb.q33c_company_name1 natural join
imdb.q33c_company_name2 natural join
imdb.q33c_info_type1 natural join
imdb.q33c_info_type2 natural join
imdb.q33c_kind_type1 natural join
imdb.q33c_kind_type2 natural join
imdb.q33c_link_type natural join
imdb.q33c_movie_info_idx1 natural join
imdb.q33c_movie_info_idx2 natural join
imdb.q33c_title1 natural join
imdb.q33c_title2 natural join
imdb.q33c_movie_companies1 natural join
imdb.q33c_movie_companies2 natural join
imdb_int.movie_link;

