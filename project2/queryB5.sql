DROP TABLE IF EXISTS T1;
DROP TABLE IF EXISTS T2;
DROP TABLE IF EXISTS T3;
DROP TABLE IF EXISTS T4;


/* select actor id of Kevin Bacon */
CREATE TABLE T1 AS SELECT id FROM actor A 
  WHERE A.fname="Kevin" AND A.lname="Bacon";

/* select all movies that Kevin Bacon stared in */
CREATE TABLE T2 AS SELECT DISTINCT C.mid AS mid FROM T1 K, casts C
  WHERE K.id=C.pid;

/* select all actors who acted with KB (including KB himself) */
CREATE TABLE T3 AS SELECT DISTINCT D.pid AS pid FROM casts D, T2 E 
  WHERE D.mid=E.mid;

/* select all movies that have actors with Bacon number 1 acted in (excluding those movie with Kevin Bacon) */
CREATE TABLE T4 AS SELECT DISTINCT F.mid AS mid FROM casts F, T3 G
  WHERE F.pid=G.pid AND F.mid NOT IN T2;

/* select all actors with Bacon number 2 */
SELECT COUNT(DISTINCT I.pid) FROM T4 H, casts I
  WHERE H.mid=I.mid AND I.pid NOT IN T3;


