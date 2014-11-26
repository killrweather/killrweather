# KillrWeather

KillrWeather is a reference application (which we are constantly improving) showing how to easily leverage and integrate [Apache Spark](http://spark.apache.org),
[Apache Cassandra](http://cassandra.apache.org), and [Apache Kafka](http://kafka.apache.org) for fast, streaming computations in asynchronous [Akka](http://akka.io) event-driven environments. This application focuses on the use case of  **[time series data](https://github.com/killrweather/killrweather/wiki/4.-Time-Series-Data-Model)**.  
  
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
    # For IntelliJ users, this creates Intellij project files
    sbt gen-idea

### Setup - 3 Steps
1. [Download the latest Cassandra](http://cassandra.apache.org/download/) and open the compressed file.


    Optional: open /apache-cassandra-{latest.version}/conf/cassandra.yaml and increase batch_size_warn_threshold_in_kb to 64

2. Start Cassandra - you may need to prepend with sudo, or chown /var/lib/cassandra. On the command line:


    ./apache-cassandra-{latest.version}/bin/cassandra -f

3. Run the setup cql scripts to create the schema and populate the weather stations table.
On the command line start a cqlsh shell:


    cd /path/to/killrweather
    ~/apache-cassandra-{latest.version}/bin/cqlsh

You should see:

    Connected to Test Cluster at 127.0.0.1:9042.
    [cqlsh {latest.version} | Cassandra {latest.version} | CQL spec {latest.version} | Native protocol {latest.version}]
    Use HELP for help.
    cqlsh>

Run the scripts:

    cqlsh> source 'create-timeseries.cql';
    cqlsh> source 'load-timeseries.cql';
    cqlsh> quit;
 
### Run 
#### To Run from an IDE
First start com.datastax.killrweather.KillrWeatherApp, then  com.datastax.killrweather.KillrWeatherClientApp.

#### To Run from Command Line

    cd /path/to/killrweather
    sbt app/run

You should see:

    Multiple main classes detected, select one to run:

    [1] com.datastax.killrweather.SimpleSparkJob
    [2] com.datastax.killrweather.KillrWeatherClientApp
    [3] com.datastax.killrweather.KillrWeatherApp

Select 3, then open a new window, do the same and select 2.
