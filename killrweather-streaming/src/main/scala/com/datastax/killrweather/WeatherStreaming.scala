package com.datastax.killrweather

import com.datastax.killrweather.Weather.RawWeatherData
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** The WeatherStreaming creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
object WeatherStreaming {

  val localLogger = Logger.getLogger("WeatherStreaming")

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()

    val KafkaTopicRaw = sparkConf.get("spark.topic.raw")
    val KafkaBroker = sparkConf.get("spark.kafka_brokers")
    val CassandraKeyspace = sparkConf.get("spark.keyspace")
    val CassandraTableRaw = sparkConf.get("spark.table.raw")
    val CassandraTableDailyPrecip = sparkConf.get("spark.table.daily.precipitation")

    println(s"using CassandraTableDailyPrecip $CassandraTableDailyPrecip")


    def createStreamingContext(): StreamingContext = {
      @transient val newSsc = new StreamingContext(sparkConf, Seconds(1))
      localLogger.info(s"Creating new StreamingContext $newSsc")
      newSsc
    }

    val sparkStreamingContext = StreamingContext.getActiveOrCreate(createStreamingContext)

    val brokers = sparkConf.get("spark.kafka_brokers", "localhost:9092")
    println(s"using kafka broker $brokers")
    val debugOutput = true

    val topics: Set[String] = KafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    localLogger.info(s"connecting to brokers: $brokers")
    localLogger.info(s"sparkStreamingContext: $sparkStreamingContext")
    localLogger.info(s"kafkaParams: $kafkaParams")
    localLogger.info(s"topics: $topics")

    import com.datastax.spark.connector.streaming._

    val rawWeatherStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sparkStreamingContext, kafkaParams, topics)
    val parsedWeatherStream = rawWeatherStream.map(_._2.split(","))
      .map(RawWeatherData(_))

    /** Saves the raw data to Cassandra - raw table. */
    parsedWeatherStream.saveToCassandra(CassandraKeyspace, CassandraTableRaw)

    /** For a given weather station, year, month, day, aggregates hourly precipitation values by day.
      * Weather station first gets you the partition key - data locality - which spark gets via the
      * connector, so the data transfer between spark and cassandra is very fast per node.
      *
      * Persists daily aggregate data to Cassandra daily precip table by weather station,
      * automatically sorted by most recent (due to how we set up the Cassandra schema:
      * @see https://github.com/killrweather/killrweather/blob/master/data/create-timeseries.cql.
      *
      * Because the 'oneHourPrecip' column is a Cassandra Counter we do not have to do a spark
      * reduceByKey, which is expensive. We simply let Cassandra do it - not expensive and fast.
      * This is a Cassandra 2.1 counter functionality ;)
      *
      * This new functionality in Cassandra 2.1.1 is going to make time series work even faster:
      * https://issues.apache.org/jira/browse/CASSANDRA-6602
      */
    parsedWeatherStream.map { weather =>
      (weather.wsid, weather.year, weather.month, weather.day, weather.oneHourPrecip)
    }.saveToCassandra(CassandraKeyspace, CassandraTableDailyPrecip)

    parsedWeatherStream.print // for demo purposes only


    //Kick off
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }

}
