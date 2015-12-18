DROP TABLE IF EXISTS Output7;

CREATE TABLE Output7(
	AttributeName VARCHAR(20), 
	CorrelationSign Integer
);

INSERT INTO Output7 VALUES ( "Temperature", (SELECT CASE WHEN SUM((B.temperature-temperature_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 ELSE -1 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(temperature) AS temperature_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );

INSERT INTO Output7 VALUES ( "FuelPrice", (SELECT CASE WHEN SUM((B.fuelprice-fuelprice_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 ELSE -1 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(fuelprice) AS fuelprice_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );

INSERT INTO Output7 VALUES ( "CPI", (SELECT CASE WHEN SUM((B.cpi-cpi_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 ELSE -1 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(cpi) AS cpi_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );

INSERT INTO Output7 VALUES ( "UnemploymentRate", (SELECT CASE WHEN SUM((B.unemploymentrate-unemploymentrate_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 ELSE -1 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(unemploymentrate) AS unemploymentrate_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );

SELECT * FROM Output7;
