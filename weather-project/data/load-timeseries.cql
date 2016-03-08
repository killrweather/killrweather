/*
Declares the keyspace to use:
*/
USE isd_weather_data;

COPY weather_station (id, name, country_code, state_code, call_sign, lat, long, elevation) FROM 'weather_stations.csv';

/*
Better to load this much data with one of the utility loading programs. But for simplicity of demos:
COPY raw_weather_data (wsid, year, month, day, hour, temperature, dewpoint, pressure, wind_direction, wind_speed, sky_condition, one_hour_precip, six_hour_precip)
FROM '2014.csv';
*/

INSERT INTO  sky_condition_lookup (code, condition) VALUES (0, 'None, SKC or CLR');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (1, 'One okta - 1/10 or less but not zero');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (2, 'Two oktas - 2/10 - 3/10, or FEW');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (3, 'Three oktas - 4/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (4, 'Four oktas - 5/10, or SCT');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (5, 'Five oktas - 6/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (6, 'Six oktas - 7/10 - 8/10');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (7, 'Seven oktas - 9/10 or more but not 10/10, or BKN');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (8, 'Eight oktas - 10/10, or OVC');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (9, 'Sky obscured, or cloud amount cannot be estimated');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (10, 'Partial obscuration 11: Thin scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (12, 'Scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (13, 'Dark scattered');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (14, 'Thin broken 15: Broken');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (16, 'Dark broken 17: Thin overcast 18: Overcast');
INSERT INTO  sky_condition_lookup (code, condition) VALUES (19, 'Dark overcast');