SELECT Y.year, SUM(M.year_count) AS decade_count FROM 
  (SELECT year, COUNT(id) AS year_count FROM movie 
	  WHERE year IS NOT NULL AND year!="" 
	  GROUP BY year) M, 
  (SELECT DISTINCT year FROM movie) Y
  WHERE M.year>=Y.year AND M.year<=Y.year+9
  GROUP BY Y.year
  ORDER BY decade_count DESC
  LIMIT 1;
