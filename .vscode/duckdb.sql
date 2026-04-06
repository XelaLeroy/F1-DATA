SELECT *
FROM telemetry
LIMIT 50;
SELECT pilot_name,
	count(*)
FROM telemetry
GROUP BY pilot_name;