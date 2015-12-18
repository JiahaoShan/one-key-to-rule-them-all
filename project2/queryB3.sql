SELECT M.year, COUNT(M.id) FROM movie M 
  WHERE M.id NOT IN (
  	SELECT DISTINCT C.mid AS mid FROM (SELECT * FROM actor WHERE gender="M") A, casts C 
  	  WHERE A.id=C.pid
  )
  GROUP BY M.year;
