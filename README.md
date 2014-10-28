# KillrWeather

KillrWeather, powered by [DataStax](http://www.datastax.com), is a reference application (in progress) showing how to easily leverage and integrate [Apache Spark](http://spark.apache.org), 
[Apache Cassandra](http://cassandra.apache.org), and [Apache Kafka](http://kafka.apache.org) for fast, streaming computations 
on **time series data** in asynchronous [Akka](http://akka.io) event-driven environments.
  
## About
KillrWeather is written in [Scala](http://www.scala-lang.org). It uses [SBT](http://www.scala-sbt.org) to build, similar to the 
[Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector), which is used as a primary dependency for easy integration of [Apache  Spark](http://spark.apache.org) and [Apache Cassandra](http://cassandra.apache.org) in a [Scala](http://www.scala-lang.org) or Java application. 

## Start Here
* Please see the [KillrWeather Wiki](https://github.com/killrweather/killrweather/wiki) for full setup instructions and documentation.
* The primary **Spark, Kafka and Cassandra code** starts (here)[https://github.com/killrweather/killrweather/tree/master/killrweather-app/src]

## Setup
1. [Set up your development environment](Environment-Prerequisites)
2. [Create the Cassandra Schema](Code & Schema Setup)
3. [The Code](Code)
4. [The Time Series Data Model](Data Model)
