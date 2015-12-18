SELECT D.fname, D.lname FROM (
	SELECT A.fname, A.lname, COUNT(DISTINCT C.mid) AS cast_count 
		FROM actor A, (SELECT id FROM movie WHERE year=2004) M, casts C 
	  WHERE A.id=C.pid AND M.id=C.mid
	  GROUP BY A.id) D
  WHERE D.cast_count>=10;
  