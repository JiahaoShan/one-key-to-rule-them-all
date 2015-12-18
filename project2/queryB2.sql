SELECT D.fname, D.lname, COUNT(DISTINCT C.mid) AS direct_count 
	FROM directors D, (SELECT * FROM movie M WHERE M.year<=2010 AND M.year>=1990) MV, movie_directors C
  WHERE D.id=C.did AND MV.id=C.mid
  GROUP BY D.id
	ORDER BY direct_count DESC
	LIMIT 100;
