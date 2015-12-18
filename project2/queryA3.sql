SELECT DISTINCT Store FROM TemporalData A 
  WHERE A.UnemploymentRate>=10 AND 
    NOT EXISTS (
    	SELECT * 
    	FROM TemporalData B 
    	WHERE B.FuelPrice>4 AND A.Store=B.Store
    );