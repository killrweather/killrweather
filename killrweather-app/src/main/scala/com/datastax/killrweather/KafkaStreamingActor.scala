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

import akka.actor.ActorRef
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._
import org.apache.spark.util.StatCounter

/** 3. The KafkaStreamActor creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class KafkaStreamingActor(kafkaParams: Map[String,String],
                          brokers: Set[String],
                          ssc: StreamingContext,
                          settings: WeatherSettings,
                          listener: ActorRef)
    extends KafkaProducerActor[String,String](brokers, settings) with WeatherActor {

  import settings._
  import WeatherEvent._
  import Weather._

  import StreamingContext._
  import SparkContext._

   val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, Map(KafkaTopicRaw -> 3), StorageLevel.MEMORY_AND_DISK_2)// TODO increase consumers
     .map { case (_, d) => d.split(",")}
     .map(RawWeatherData(_))

  /** Saves the raw data to Cassandra - raw table. */
  stream.saveToCassandra(CassandraKeyspace, CassandraTableRaw)


  /* Use StreamingContext.remember to specify how much of the past data to be "remembered" (i.e., kept around, not deleted).
     Otherwise, data will get deleted and slice will fail.
     Slices over data within the last hour, then you should call remember (Seconds(1)). */
  ssc.remember(Seconds(60))

  /** For the given day of the year, aggregates hourly precipitation values by day.
    * Persists to Cassandra daily precip table by weather station.
    * Because the 'oneHourPrecip' column is a Cassandra Counter,
    * we do not have to do a spark reduceByKey, which is expensive!
    */
  stream
    .map(h => (h.wsid,h.year,h.month,h.day,h.oneHourPrecip))
    .saveToCassandra(CassandraKeyspace, CassandraTableDailyPrecip)

  /**
   * For the given day of the year, aggregates all the temp values to statistics:
   * high, low, mean, std, etc.
   * Persists to Cassandra daily temperature table by weather station.
   * stream.slice(fromTime = Time(start), toTime = Time(end))
   */
  val temps = stream.map(t => DayKey(t) -> StatCounter(Seq(t.temperature)))
    temps.reduceByKey(_ merge _)
    .map { case (time, stats) => DailyTemperature(time, stats) }
    .saveToCassandra(CassandraKeyspace, CassandraTableDailyTemp)

  /** Notifies the supervisor that the Spark Streams have been created and defined.
    * Now the [[StreamingContext]] can be started. */
  listener ! OutputStreamInitialized

}
