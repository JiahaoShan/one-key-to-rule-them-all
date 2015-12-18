# README

Buzun Chen
Yexin Wu

We used composite indexes (indexes with multiple columns) to optimize the performance of queries with multiple AND constraints in WHERE clauses.
Here are the indexes that we created:

1. cast_index 
	CREATE INDEX cast_index ON casts(pid, mid);

	This index speeds up query 1, 2, 3, 4, 5. This is because those queries involve WHERE clauses that check casts.pid=XX AND casts.mid=XX, or just casts.pid=XX

2. cast_inverse_index
	CREATE INDEX cast_inverse_index ON casts(mid, pid);

	This index speeds up query 5. We added this index because it is different from the first one above. For multi-column indeces, Sqlite3 will create multiple lookup tables hierarchically and lookup them in order. Therefore, if we only have cast_index, we will miss those WHERE clauses that only check for casts.mid=XX, or those put casts.mid=XX before casts.pid=XX.

3. actor_index
	CREATE INDEX actor_index ON actor(id);

	This index speeds up query 1, 3. Because in query 1 and 3 we have WHERE clauses actor.id=XX.

4. actor_gender_index
	CREATE INDEX actor_gener_index ON actor(gender);

	This index speeds up query 3. Because in query 3 we have a WHERE clause actor.gender=XX.

5. actor_name_index
	CREATE INDEX actor_name_index ON actor(fname, lname);

	This index speeds up query 5. In query 5 we use WHERE actor.fname="Kevin" AND actor.lname="Bacon".

6. movie_index
	CREATE INDEX movie_index ON movie(id, year);

	This index speeds up query 1, 2, 3, 4. Because there are WHERE clauses movie.id=XX.

7. movie_year_index
	CREATE INDEX movie_year_index ON movie(year);

	This index speeds up query 1, 2, 4. Because there are WHERE clauses movie.year=XXXX.

8. directors_index
	CREATE INDEX directors_index ON directors(id);

	This index speeds up query 2. Because there are WHERE clauses directors.id=XX.

9. movie_directors_index
	CREATE INDEX movie_directors_index ON movie_directors(did, mid);

	This index speeds up query 2. Because there are WHERE clauses movie_directors.did=XX AND movie_directors.mid=XX.


## Execution Time (Part B)

	**times are recorded by using `.timer on` in sqlite3 commandline**

	* Query 1

	without indexes: [30.848, 30.767, 31.476] avg=31.030
	with indexes: [4.566, 4.796, 4.807] avg=4.723

	* Query 2

	without indexes: [5.723, 5.737, 5.799] avg=5.753
	with indexes: [4.303, 4.509, 4.464] avg=4.425

	* Query 3

	without indexes: [35.234, 36.822, 33.770] avg=35.275
	with indexes: [19.909, 20.202, 20.647] avg=20.253

	* Query 4

	without indexes: [1.023, 1.047, 1.025] avg=3.095
	with indexes: [0.953, 0.944, 0.959] avg=0.952

	* Query 5

	without indexes: [37.322, 33.364, 37.552] avg=36.079
	with indexes: [14.892, 15.036, 14.541] avg=14.823

	

