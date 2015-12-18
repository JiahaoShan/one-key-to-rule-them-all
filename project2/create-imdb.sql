-- Imports the IMDB movie dataset

PRAGMA foreign_keys = ON;

CREATE TABLE actor(
  id int PRIMARY KEY,
  fname varchar(30),
  lname varchar(30),
  gender char(1));

CREATE TABLE movie(
  id int PRIMARY KEY,
  name varchar(150),
  year int);

CREATE TABLE directors(
  id int PRIMARY KEY,
  fname varchar(30),
  lname varchar(30));

CREATE TABLE genre(
  mid int,
  genre varchar(50));

CREATE TABLE  casts(
  pid int REFERENCES actor,
  mid int REFERENCES movie,
  role varchar(50));

CREATE TABLE  movie_directors(
  did int REFERENCES directors,
  mid int REFERENCES movie);

.import 'imdb/actor.txt' actor
.import 'imdb/movie.txt' movie
.import 'imdb/casts.txt' casts
.import 'imdb/directors.txt' directors
.import 'imdb/genre.txt' genre
.import 'imdb/movie_directors.txt' movie_directors
