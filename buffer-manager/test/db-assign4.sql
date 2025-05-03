
-- make sure to change the path to your own path
-- make sure to run preprocessing script first
-- db-assign4.sql
-- Run in psql:
--   CREATE DATABASE imdb;
--   \c imdb
--   psql -U <user> -d imdb db-assign4.sql

\encoding UTF8

DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS workedon;
DROP TABLE IF EXISTS people;

CREATE TABLE movies (
  movieid CHAR(9),
  title   CHAR(30)
);

CREATE TABLE workedon (
  movieid  CHAR(9),
  personid CHAR(10),
  category CHAR(20)
);

CREATE TABLE people (
  personid CHAR(10),
  name     CHAR(105)
);

\COPY movies(movieid, title) FROM 'C:/Users/Tanvi/Desktop/movies_pre.tsv' WITH (FORMAT csv, DELIMITER E'\t', HEADER true, ENCODING 'UTF8');

\COPY workedon(movieid, personid, category) FROM 'C:/Users/Tanvi/Desktop/workedon_pre.tsv' WITH (FORMAT csv, DELIMITER E'\t', HEADER true, ENCODING 'UTF8');

\COPY people(personid, name) FROM 'C:/Users/Tanvi/Desktop/people_pre.tsv' WITH (FORMAT csv, DELIMITER E'\t', HEADER true, ENCODING 'UTF8');

\COPY (SELECT TRIM(m.title) AS title, TRIM(p.name)  AS name FROM movies AS m JOIN workedon  AS w ON m.movieid  = w.movieid JOIN people AS p ON w.personid = p.personid WHERE TRIM(m.title) BETWEEN 'A-Hunting We Will Go' AND 'A-Wol' AND TRIM(w.category) = 'director' ORDER BY title, name) TO 'C:/Users/Tanvi/Desktop/verify_output1.csv' WITH (FORMAT csv, HEADER true);

\COPY (  SELECT TRIM(m.title) AS title, TRIM(p.name) AS name FROM movies AS m JOIN workedon  AS w ON m.movieid  = w.movieid JOIN people AS p ON w.personid = p.personid WHERE TRIM(m.title) BETWEEN 'A-Hunting We Will Go' AND 'A Bad Case' AND TRIM(w.category) = 'director' ORDER BY title, name) TO 'C:/Users/Tanvi/Desktop/verify_output2.csv' WITH (FORMAT csv, HEADER true);

\COPY ( SELECT TRIM(m.title) AS title, TRIM(p.name)  AS name FROM movies    AS m JOIN workedon  AS w ON m.movieid  = w.movieid JOIN people AS p ON w.personid = p.personid WHERE TRIM(m.title) BETWEEN 'A-Hunting We Will Go' AND 'A Corner in Wheat' AND TRIM(w.category) = 'director' ORDER BY title, name) TO 'C:/Users/Tanvi/Desktop/verify_output3.csv' WITH (FORMAT csv, HEADER true);
