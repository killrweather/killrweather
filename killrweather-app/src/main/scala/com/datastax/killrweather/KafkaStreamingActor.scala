/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.killrweather

import akka.actor.{Actor, ActorRef}
import akka.cluster.Cluster
import com.datastax.spark.connector.embedded.KafkaEvent.KafkaMessageEnvelope
import com.datastax.spark.connector.embedded.{KafkaEvent, KafkaProducerActor}
import kafka.producer.ProducerConfig
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._

/** The KafkaStreamActor creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class KafkaStreamingActor(kafkaParams: Map[String, String],
                          ssc: StreamingContext,
                          settings: WeatherSettings,
                          listener: ActorRef) extends AggregationActor {

  import settings._
  import WeatherEvent._
  import Weather._

  val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, Map(KafkaTopicRaw -> 10), StorageLevel.DISK_ONLY_2)
    .map { case (_, line) => line.split(",")}
    .map(RawWeatherData(_))

  /** Saves the raw data to Cassandra - raw table. */
  kafkaStream.saveToCassandra(CassandraKeyspace, CassandraTableRaw)

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
  kafkaStream.map { weather =>
    (weather.wsid, weather.year, weather.month, weather.day, weather.oneHourPrecip)
  }.saveToCassandra(CassandraKeyspace, CassandraTableDailyPrecip)

  /** Notifies the supervisor that the Spark Streams have been created and defined.
    * Now the [[StreamingContext]] can be started. */
  listener ! OutputStreamInitialized

  def receive : Actor.Receive = {
    case e => // ignore
  }
}


/** The KafkaPublisherActor loads data from /data/load files on startup (because this
  * is for a runnable demo) and also receives [[KafkaMessageEnvelope]] messages and
  * publishes them to Kafka on a sender's behalf.
  *
  * [[KafkaMessageEnvelope]] messages sent to this actor are handled by the [[KafkaProducerActor]]
  * which it extends.
  */
class KafkaPublisherActor(val producerConfig: ProducerConfig,
                          sc: SparkContext,
                          settings: WeatherSettings) extends KafkaProducerActor[String, String] {

  import settings.{KafkaTopicRaw => topic, KafkaGroupId => group}
  import settings._
  import KafkaEvent._

  log.info("Starting data file ingestion on {}", Cluster(context.system).selfAddress)

  val toActor = (data: String) => self ! KafkaMessageEnvelope[String,String](topic, group, data)

  /** Because we run locally vs against a cluster as a demo app, we keep that file size data small.
    * Using rdd.toLocalIterator will consume as much memory as the largest partition in this RDD,
    * which in this use case is 360 or fewer (if current year before December 31) small Strings.
    *
    * The ingested data is sent to the kafka actor for processing in the stream.
    *
    * RDD.toLocalIterator will consume as much memory as the largest partition in this RDD.
    * RDD.toLocalIterator uses allowLocal = false flag. `allowLocal` specifies whether the
    * scheduler can run the computation on the driver rather than shipping it out to the cluster
    * for short actions like first().
    */
  for (file <- IngestionData) {
    log.info(s"Ingesting $file")
    sc.textFile(file).flatMap(_.split("\\n")).toLocalIterator.foreach(toActor)
  }

}
