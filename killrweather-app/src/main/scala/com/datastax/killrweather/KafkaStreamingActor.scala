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

import akka.actor.{ActorRef, Actor}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import kafka.server.KafkaConfig
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.util.StatCounter
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.actor.{KafkaProducer, WeatherActor}

/** 3. The KafkaStreamActor creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class KafkaStreamingActor(val config: KafkaConfig,
                          kafkaParams: Map[String,String],
                          ssc: StreamingContext,
                          settings: WeatherSettings, listener: ActorRef)
                          extends WeatherActor with KafkaProducer {

  import settings.{ CassandraKeyspace => keyspace }
  import settings._
  import WeatherEvent._
  import Weather._

  /* Using rdd.toLocalIterator will consume as much memory as the largest partition in this RDD */
  for (year <- DataYearRange) {
    val location = s"$DataLoadPath/$year.csv.gz"
    log.info(s"Attempting to read $location")

    ssc.sparkContext.textFile(location)
      .flatMap(_.split("\\n"))
      .toLocalIterator
      .map(send(KafkaTopicRaw, KafkaGroupId, _))
  }

  val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, Map(KafkaTopicRaw -> 1), StorageLevel.MEMORY_ONLY)
    .map { case (_, d) => d.split(",")}
    .map(RawWeatherData(_))
    // once we convert the data we can not break off in the stream - serialization errors
    // we need to persist the raw before we can start aggregating bcz aggregation requires
    // the raw to already be stored
    .saveToCassandra(CassandraKeyspace, CassandraTableRaw)

  listener ! OutputStreamInitialized

  /** For the given day of the year, aggregates all the temp values to statistics:
    * high, low, mean, std, etc.
    * Persists to Cassandra daily temperature table by weather station.
    *
    * For the given day of the year, aggregates all the precip values to statistics.
    * Persists to Cassandra daily precip table by weather station.
    */
  def aggregate(data: RawWeatherData): Unit = {
    val wsid = data.weatherStation
    val dt = s"${data.month}/${data.day}/${data.year}"
    log.debug(s"Computing ${data.month}/${data.day}/${data.year} for $wsid")

    val toDailyTemperature = (s: StatCounter) => DailyTemperature(data, s)

    val dataStream = ssc.cassandraTable[(String, Double)](keyspace, CassandraTableRaw)

    dataStream.select("weather_station", "temperature")
      .where("weather_station = ? AND year = ? AND month = ? AND day = ?",
        data.weatherStation, data.year, data.month, data.day)
      .mapValues { t => StatCounter(Seq(t))}
      .reduceByKey(_ merge _)
      .map { case (_, s) => toDailyTemperature(s)}
      .saveToCassandra(keyspace, CassandraTableDailyTemp)

    dataStream.select("weather_station", "temperature")
      .select("weather_station", "precipitation")
      .where("weather_station = ? AND year = ? AND month = ? AND day = ?",
        data.weatherStation, data.year, data.month, data.day)
      .map { case (_, p) => DailyPrecipitation(data, p)}
      .saveToCassandra(keyspace, CassandraTableDailyPrecip)
  }

  def receive: Actor.Receive = {
    case _ =>
  }
}
