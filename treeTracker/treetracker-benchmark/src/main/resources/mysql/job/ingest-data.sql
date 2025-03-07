-- Reference: https://stackoverflow.com/q/2675323/1460102
SET GLOBAL local_infile=1;
use imdb;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/name.csv' INTO TABLE imdb.name FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY
    "\n" (id, name, imdb_index, @vimdb_id, gender, name_pcode_cf, name_pcode_nf, surname_pcode, md5sum) SET imdb_id = NULLIF(@vimdb_id,'');

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_name.csv'
    INTO TABLE imdb.aka_name
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n";

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/aka_title.csv'
    INTO TABLE imdb.aka_title
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
    (id, movie_id, title, imdb_index, kind_id, @vproduction_year, phonetic_code, @vepisode_of_id, @vseason_nr, @vepisode_nr, note, md5sum)
    SET
        episode_of_id = NULLIF(@vepisode_of_id,''),
        season_nr = NULLIF(@vseason_nr, ''),
        episode_nr = NULLIF(@vepisode_nr, ''),
        production_year = NULLIF(@vproduction_year, '')
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/cast_info.csv'
    INTO TABLE imdb.cast_info
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
    (id, person_id,movie_id,@vperson_role_id,note,@vnr_order,role_id)
    SET
        nr_order = NULLIF(@vnr_order,''),
        person_role_id = NULLIF(@vperson_role_id, '')
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/char_name.csv'
    INTO TABLE imdb.char_name
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
    (id,name,imdb_index,@vimdb_id,name_pcode_nf,surname_pcode,md5sum)
    SET
        imdb_id = NULLIF(@vimdb_id, '')
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/comp_cast_type.csv'
    INTO TABLE imdb.comp_cast_type
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/company_name.csv'
    INTO TABLE imdb.company_name
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
    (id,name,country_code,@vimdb_id,name_pcode_nf,name_pcode_sf,md5sum)
    SET
        imdb_id = NULLIF(@vimdb_id, '')
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/company_type.csv'
    INTO TABLE imdb.company_type
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/complete_cast.csv'
    INTO TABLE imdb.complete_cast
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/info_type.csv'
    INTO TABLE imdb.info_type
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/keyword.csv'
    INTO TABLE imdb.keyword
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/kind_type.csv'
    INTO TABLE imdb.kind_type
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/link_type.csv'
    INTO TABLE imdb.link_type
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_companies.csv'
    INTO TABLE imdb.movie_companies
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info_idx.csv'
    INTO TABLE imdb.movie_info_idx
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_keyword.csv'
    INTO TABLE imdb.movie_keyword
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_link.csv'
    INTO TABLE imdb.movie_link
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/role_type.csv'
    INTO TABLE imdb.role_type
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

-- We modify row 2514451 because "\Frag'ile\" in the original data set cannot be imported correctly to mysql. This is the
-- only row we changed compared to the original data set.
LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/title_mysql.csv'
    INTO TABLE imdb.title
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
    (id,title,imdb_index,kind_id,@vproduction_year,@vimdb_id,phonetic_code,@vepisode_of_id,@vseason_nr,@vepisode_nr,series_years,md5sum)
    SET
        imdb_id = NULLIF(@vimdb_id, ''),
        season_nr = NULLIF(@vseason_nr, ''),
        episode_nr = NULLIF(@vepisode_nr, ''),
        production_year = NULLIF(@vproduction_year, ''),
        episode_of_id = NULLIF(@vepisode_of_id, '')
;

LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/movie_info.csv'
    INTO TABLE imdb.movie_info
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;

-- We modify row 2671662 because '\' in 'Daughter of Irish actor and raconteur 'Niall Toibin' (qv); \,'. This is the only row
-- we modified.
LOAD DATA LOCAL INFILE '/home/zeyuanhu/projects/job/imdb2013/imdb/person_info_mysql.csv'
    INTO TABLE imdb.person_info
    FIELDS TERMINATED BY ","
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY "\n"
;