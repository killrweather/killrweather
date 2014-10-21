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
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.SparkContext
import org.apache.spark.util.StatCounter
import kafka.server.KafkaConfig
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.actor.KafkaProducer
import com.datastax.spark.connector._

/** 3. The KafkaStreamActor creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class KafkaStreamingActor(val config: KafkaConfig,
                          kafkaParams: Map[String,String],
                          ssc: StreamingContext,
                          settings: WeatherSettings, listener: ActorRef)
                          extends KafkaProducer {

  import settings.{ CassandraKeyspace => keyspace }
  import settings._
  import WeatherEvent._
  import Weather._
  import StreamingContext._
  import SparkContext._

  /* Loads historic data from /data/load files. Because we run locally vs against a cluster
   * solely for purposes of a demo app, we keep that file size data small.
   * Using rdd.toLocalIterator will consume as much memory as the largest partition in this RDD,
   * which in this use case is 360 or fewer (if current year before December 31) small Strings. */
  for (partition <- ByYearPartitions) {
    val toKafka = (line: String) => send(KafkaTopicRaw, KafkaGroupId, line)

    ssc.sparkContext.textFile(partition.uri)
      .flatMap(_.split("\\n"))
      .toLocalIterator
      .foreach(toKafka)
  }

  /* Use StreamingContext.remember to specify how much of the past data to be "remembered" (i.e., kept around, not deleted).
     Otherwise, data will get deleted and slice will fail.
     Slices over data within the last hour, then you should call remember (Seconds(1)). */
  ssc.remember(Seconds(60))

   val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, Map(KafkaTopicRaw -> 1), StorageLevel.MEMORY_ONLY)
    .map { case (_, d) => d.split(",")}
    .map(RawWeatherData(_))
     // temporary - just making the data more insteresting since the truncated
     // file currently just has 0.0 for all one hour precips
    .map(p => if(p.day == 2) p.copy(oneHourPrecip = 1.0) else p)

  /** Saves the raw data to Cassandra - raw table. */
  stream.saveToCassandra(keyspace, CassandraTableRaw)

  /** For the given day of the year, aggregates hourly precipitation values by day.
    * Persists to Cassandra daily precip table by weather station. */
  stream
    .map(hourly => (hourly.weatherStation,hourly.year,hourly.month,hourly.day,hourly.oneHourPrecip))
    .saveToCassandra(keyspace, CassandraTableDailyPrecip)

  /** For the given day of the year, aggregates all the temp values to statistics:
    * high, low, mean, std, etc.
    * Persists to Cassandra daily temperature table by weather station.
    */
  // stream.slice(fromTime = Time(start), toTime = Time(end))
  /* val temps = rddByDay.map(t => DayKey(t) -> StatCounter(Seq(t.temperature)))
            temps.reduceByKey(_ merge _)
            .map { case (k, s) => DailyTemperature(k,s) }
            .saveToCassandra(keyspace, CassandraTableDailyTemp) */

  listener ! OutputStreamInitialized

  def receive: Actor.Receive = {
    case _ =>
  }
}
