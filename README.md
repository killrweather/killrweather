KillrWeather
============

The killrweather.com app. Powerd by DataStax. An example application web site to highlight integration between Apache Spark and Apache Cassandra.

## Data Model

You can find any schema creation scripts [here](https://github.com/killrweather/killrweather/tree/master/data)

####create-timeseries.cql
Basic schema for creation of Cassandra keyspace and tables for storing raw weather data from ISD-lite hourly files.

 - weather_station: Lookup table for weather station by code. For this data file, we combine two fields in the format xxxxxx:yyyyy xxxxx is the 
