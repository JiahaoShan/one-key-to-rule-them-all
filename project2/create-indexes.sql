DROP INDEX IF EXISTS cast_index;
DROP INDEX IF EXISTS cast_inverse_index;
DROP INDEX IF EXISTS actor_index;
DROP INDEX IF EXISTS actor_name_index;
DROP INDEX IF EXISTS actor_gender_index;
DROP INDEX IF EXISTS movie_index;
DROP INDEX IF EXISTS movie_year_index;
DROP INDEX IF EXISTS directors_index;
DROP INDEX IF EXISTS movie_directors_index;

CREATE INDEX cast_index ON casts(pid, mid);
CREATE INDEX cast_inverse_index ON casts(mid, pid);
CREATE INDEX actor_index ON actor(id);
CREATE INDEX actor_gener_index ON actor(gender);
CREATE INDEX actor_name_index ON actor(fname, lname);
CREATE INDEX movie_index ON movie(id, year);
CREATE INDEX movie_year_index ON movie(year);
CREATE INDEX directors_index ON directors(id);
CREATE INDEX movie_directors_index ON movie_directors(did, mid);
