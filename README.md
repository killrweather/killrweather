KillrWeather
============

The killrweather.com app. Powerd by DataStax. An example application web site to highlight integration between Apache Spark and Apache Cassandra.

## Data Model

You can find any schema creation scripts [here](https://github.com/killrweather/killrweather/tree/master/data)

####create-timeseries.cql
Basic schema for creation of Cassandra keyspace and tables for storing raw weather data from ISD-lite hourly files.

 - weather_station: Lookup table for weather station by code. For this data file, we combine two fields in the format xxxxxx:yyyyy xxxxx is the 
=======
KillrWeather, powered by [DataStax](http://www.datastax.com), is about how to easily leverage and integrate [Apache Spark](http://spark.apache.org), 
[Apache Cassandra](http://cassandra.apache.org), and [Apache Kafka](http://kafka.apache.org) for fast, streaming computations 
on **time series data** in asynchronous [Akka](http://akka.io) event-driven environments.
  
KillrWeather is written in [Scala](http://www.scala-lang.org). It uses [SBT](http://www.scala-sbt.org) to build, similar to the 
[Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector), 
which is used as a primary dependency for easy integration of [Apache  Spark](http://spark.apache.org) and [Apache Cassandra](http://cassandra.apache.org) 
in a [Scala](http://www.scala-lang.org) or Java application. 

Please see the [KillrWeather Wiki](https://github.com/killrweather/killrweather/wiki) for full setup instructions and documentation.

