SELECT Dept, SUM(WeeklySales/Size) AS TotalSales
  FROM (Stores NATURAL JOIN Sales)
  GROUP BY Dept
  ORDER BY TotalSales DESC
  LIMIT 10
;
