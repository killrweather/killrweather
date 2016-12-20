# KillrWeather

KillrWeather is a reference application (which we are constantly improving) showing how to easily leverage and integrate [Apache Spark](http://spark.apache.org),
[Apache Cassandra](http://cassandra.apache.org), and [Apache Kafka](http://kafka.apache.org) for fast, streaming computations in asynchronous [Akka](http://akka.io) event-driven environments. This application focuses on the use case of  **[time series data](https://github.com/killrweather/killrweather/wiki/4.-Time-Series-Data-Model)**.  
  
## Sample Use Case
I need fast access to historical data  on the fly for  predictive modeling  with real time data from the stream. 

## Basic Samples
[Basic Spark, Kafka, Cassandra Samples](https://github.com/killrweather/killrweather/tree/master/killrweather-examples/src/main/scala/com/datastax/killrweather)

## Reference Application 
[KillrWeather Main App](https://github.com/killrweather/killrweather/tree/master/killrweather-app/src/main/scala/com/datastax/killrweather)

## Time Series Data 
The use of time series data for business analysis is not new. What is new is the ability to collect and analyze massive volumes of data in sequence at extremely high velocity to get the clearest picture to predict and forecast future market changes, user behavior, environmental conditions, resource consumption, health trends and much, much more.

Apache Cassandra is a NoSQL database platform particularly suited for these types of Big Data challenges. Cassandra’s data model is an excellent fit for handling data in sequence regardless of data type or size. When writing data to Cassandra, data is sorted and written sequentially to disk. When retrieving data by row key and then by range, you get a fast and efficient access pattern due to minimal disk seeks – time series data is an excellent fit for this type of pattern. Apache Cassandra allows businesses to identify meaningful characteristics in their time series data as fast as possible to make clear decisions about expected future outcomes.

There are many flavors of time series data. Some can be windowed in the stream, others can not be windowed in the stream because queries are not by time slice but by specific year,month,day,hour. Spark Streaming lets you do both.

## Start Here
* [KillrWeather Wiki](https://github.com/killrweather/killrweather/wiki) 
* com.datastax.killrweather [Spark, Kafka and Cassandra workers](http://github.com/killrweather/killrweather/tree/master/killrweather-app/src/it/scala/com/datastax/killrweather)

### Clone the repo

    git clone https://github.com/killrweather/killrweather.git
    cd killrweather


### Build the code 
If this is your first time running SBT, you will be downloading the internet.

    cd killrweather
    sbt compile
    # For IntelliJ users, this creates Intellij project files, but as of
    # version 14x you should not need this, just import a new sbt project.
    sbt gen-idea

### Setup (for Linux & Mac) - 3 Steps
1.[Download the latest Cassandra](http://cassandra.apache.org/download/) and open the compressed file.

2.Start Cassandra - you may need to prepend with sudo, or chown /var/lib/cassandra. On the command line:


    ./apache-cassandra-{version}/bin/cassandra -f

3.Run the setup cql scripts to create the schema and populate the weather stations table.
On the command line start a cqlsh shell:

    cd /path/to/killrweather/data
    path/to/apache-cassandra-{version}/bin/cqlsh

### Setup (for Windows) - 3 Steps
1. [Download the latest Cassandra](http://www.planetcassandra.org/cassandra) and double click the installer.

2. Chose to run the Cassandra automatically during start-up

3. Run the setup cql scripts to create the schema and populate the weather stations table.
On the command line start a `cqlsh` shell:

```
    cd c:/path/to/killrweather
    c:/pat/to/cassandara/bin/cqlsh
```

### In CQL Shell:
You should see:

     Connected to Test Cluster at 127.0.0.1:9042.
     [cqlsh {latest.version} | Cassandra {latest.version} | CQL spec {latest.version} | Native protocol {latest.version}]
     Use HELP for help.
     cqlsh>

Run the scripts, then keep the cql shell open querying once the apps are running:

     cqlsh> source 'create-timeseries.cql';
     cqlsh> source 'load-timeseries.cql';


### Run
#### Logging
You will see this in all 3 app shells because log4j has been explicitly taken off the classpath:

    log4j:WARN No appenders could be found for logger (kafka.utils.VerifiableProperties).
    log4j:WARN Please initialize the log4j system properly.

What we are really trying to isolate here is what is happening in the apps with regard to the event stream.
You can add log4j locally.

To change any package log levels and see more activity, simply modify
- [logback.xml](http://github.com/killrweather/killrweather/tree/master/killrweather-core/src/resources/logback.xml)

#### From Command Line
1.Start `KillrWeather`

    cd /path/to/killrweather
    sbt app/run

As the `KillrWeather` app initializes, you will see Akka Cluster start, Zookeeper and the Kafka servers start.

For all three apps in load-time you see the Akka Cluster node join and start metrics collection. In deployment with multiple nodes of each app
this would leverage the health of each node for load balancing as the rest of the cluster nodes join the cluster:

2.Start the Kafka data feed app
In a second shell run:

    sbt clients/run

You should see:

    Multiple main classes detected, select one to run:

    [1] com.datastax.killrweather.KafkaDataIngestionApp
    [2] com.datastax.killrweather.KillrWeatherClientApp

Select `KafkaDataIngestionApp`, and watch the shells for activity. You can stop the data feed or let it keep running.
After a few seconds you should see data by entering this in the cqlsh shell:

    cqlsh> select * from isd_weather_data.raw_weather_data;

This confirms that data from the ingestion app has published to Kafka, and that raw data is
streaming from Spark to Cassandra from the `KillrWeatherApp`.

    cqlsh> select * from isd_weather_data.daily_aggregate_precip;

Unfortunately the precips are mostly 0 in the samples (To Do).

3.Open a third shell and again enter this but select `KillrWeatherClientApp`:

    sbt clients/run
This api client runs queries against the raw and the aggregated data from the kafka stream.
It sends requests (for varying locations and dates/times) and for some, triggers further aggregations
in compute time which are also saved to Cassandra:

* current weather
* daily temperatures
* monthly temperatures
* monthly highs and low temperatures
* daily precipitations
* top-k precipitation

Next I will add some forecasting with ML :)

Watch the app and client activity in request response of weather data and aggregation data.
Because the querying of the API triggers even further aggregation of data from the originally
aggregated daily roll ups, you can now see a new tier of temperature and precipitation aggregation:
In the cql shell:

    cqlsh> select * from isd_weather_data.daily_aggregate_temperature;
    cqlsh> select * from isd_weather_data.daily_aggregate_precip;

#### From an IDE
1. Run the app [com.datastax.killrweather.KillrWeatherApp](https://github.com/killrweather/killrweather/blob/master/killrweather-app/src/main/scala/com/datastax/killrweather/KillrWeatherApp.scala)
2. Run the kafka data ingestion server [com.datastax.killrweather.KafkaDataIngestionApp](https://github.com/killrweather/killrweather/blob/master/killrweather-clients/src/main/scala/com/datastax/killrweather/KafkaDataIngestionApp.scala)
3. Run the API client [com.datastax.killrweather.KillrWeatherClientApp](https://github.com/killrweather/killrweather/blob/master/killrweather-clients/src/main/scala/com/datastax/killrweather/KillrWeatherClientApp.scala)

To close the cql shell:

    cqlsh> quit;
