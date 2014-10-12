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

import scala.concurrent.Future
import akka.pattern.{ pipe, ask }
import akka.actor.{Actor, ActorRef}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import kafka.server.KafkaConfig
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.killrweather.actor.{KafkaProducer, WeatherActor}
import WeatherEvent._
import Weather._

/** For a given weather station id, retrieves the full station data. */
class WeatherStationActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableStations => weatherstations}

  def receive : Actor.Receive = {
    case GetWeatherStation(sid)  => weatherStation(sid, sender)
    case StreamWeatherStationIds => streamWeatherStationIds(sender)
    case GetWeatherStationIds    => weatherStationIds(sender)
  }

  /** 1. Streams weather station Ids to the daily computation actors.
    * Requires less heap memory and system load, but is slower than collectAsync below. */
  def streamWeatherStationIds(requester: ActorRef): Unit =
    ssc.cassandraTable[String](keyspace, weatherstations).select("id")
      .toLocalIterator foreach (id => requester ! WeatherStationIds(id))

  /** 1. Collects weather station Ids async, to the daily computation actors.
    * Requires more heap memory and system load, but is faster than toLocalIterator above. */
  def weatherStationIds(requester: ActorRef): Unit =
    for (stations <- ssc.cassandraTable[String](keyspace, weatherstations).select("id").collectAsync)
    yield requester ! WeatherStationIds(stations: _*)

  /** The reason we can not allow a `LIMIT 1` in the `where` function is that
    * the query is executed on each node, so the limit would applied in each
    * query invocation. You would probably receive about partitions_number * limit results.
    */
  def weatherStation(sid: String, requester: ActorRef): Unit =
    for {
      stations <- ssc.cassandraTable[Weather.WeatherStation](keyspace, weatherstations)
        .where("id = ?", sid)
        .collectAsync
      station <- stations.headOption
    } requester ! station

}

/** 2. The RawDataPublisher transforms annual weather .gz files to line data and publishes to a Kafka topic.
  *
  * This just batches one data file for the demo. But you could do something like this
  * to set up a monitor on an S3 bucket, so that when new files arrive, Spark streams
  * them in. New files are read as text files with 'textFileStream' (using key as LongWritable,
  * value as Text and input format as TextInputFormat)
  * {{{
  *   streamingContext.textFileStream(dir)
       .reduceByWindow(_ + _, Seconds(30), Seconds(1))
  * }}}
  */
class RawDataPublisher(val config: KafkaConfig, ssc: StreamingContext, settings: WeatherSettings) extends KafkaProducer {
  import settings._

  def receive : Actor.Receive = {
    case PublishFeed(years) => publish(years, sender)
  }

  def publish(years: Range, requester: ActorRef): Unit =
    years foreach { n =>
      val location = s"$DataLocation$n.$DataFormat"

      /* The iterator will consume as much memory as the largest partition in this RDD */
      val lines = ssc.sparkContext.textFile(location).flatMap(_.split("\\n")).toLocalIterator
      println(s"\n\n*** lines has ${lines.size}")
      batchSend(KafkaTopicRaw, KafkaGroupId, KafkaBatchSendSize, lines.toSeq)
    }
}

/** 3. The KafkaStreamActor elegantly creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class KafkaStreamActor(kafka: EmbeddedKafka, ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings._

  /* Creates the Kafka stream and defines the work to be done. */
  val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafka.kafkaParams, Map(KafkaTopicRaw -> 1), StorageLevel.MEMORY_ONLY)

  stream.map { case (_,d) => d.split(",")}
    .map (RawWeatherData(_))
    .saveToCassandra(CassandraKeyspace, CassandraTableRaw)

  def receive : Actor.Receive = {
    case _ =>
  }
}

/** 5. For a given weather station, calculates annual cumulative precip - or year to date. */
class PrecipitationActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableDailyPrecip => dailytable}

  implicit def ordering: Ordering[(String,Double)] = Ordering.by(_._2)

  def receive : Actor.Receive = {
    case GetPrecipitation(sid, year) => compute(sid, year, sender)
    case GetTopKPrecipitation(year) => topk(year, sender)
  }

  /** Returns a future value to the `requester` actor.
    * Precip values are 1 hour deltas from the previous. */
  def compute(sid: String, year: Int, requester: ActorRef): Unit = {
    val dt = timestamp.withYear(year)
    for {
      precip <- ssc.cassandraTable[Double](keyspace, dailytable)
        .select("oneHourPrecip") // TODO change field name
        .where("id = ? AND year = ?", sid, year)
        .collectAsync
    } yield Precipitation(sid, precip) // at most 365 values if available
  } pipeTo requester

  /** Returns the 10 highest temps for any station in the `year`. */
  def topk(year: Int, requester: ActorRef): Unit = Future {
    val top = ssc.cassandraTable[(String,Double)](keyspace, dailytable)
      .select("id","precip") // TODO change field name
      .where("year = ?", year)
      .top(10)

    TopKPrecipitation(top)
  } pipeTo requester

}
