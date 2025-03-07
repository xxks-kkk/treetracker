/*+ Leading( (((((ml lt) t) t2) mk) k) )
    HashJoin(ml lt t t2 mk k)
*/
SELECT MIN(lt.link) AS link_type,
       MIN(t.title) AS first_movie,
       MIN(t2.title) AS second_movie
FROM keyword AS k,
     link_type AS lt,
     movie_keyword AS mk,
     movie_link AS ml,
     title AS t,
     title AS t2
WHERE k.keyword ='10,000-mile-club'
  AND mk.keyword_id = k.id
  AND t.id = mk.movie_id
  AND ml.movie_id = t.id
  AND ml.linked_movie_id = t2.id
  AND lt.id = ml.link_type_id
  AND mk.movie_id = t.id;

