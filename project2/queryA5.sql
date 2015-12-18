SELECT Type, SUBSTR(WeekDate,6,2) AS Month, SUM(WeeklySales) AS MonthSales
	FROM (Stores NATURAL JOIN Sales)
	GROUP BY Type, Month;
		