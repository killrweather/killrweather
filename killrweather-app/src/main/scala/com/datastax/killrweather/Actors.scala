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
import com.datastax.killrweather.actor.{KafkaProducer, WeatherActor}

/** Downloads the annual weather .gz files, transforms to line data,
  * and publishes to Kafka topic.
  * Stores the raw data in Cassandra for multi-purpose data analysis.
  *
  * This just batches one data file for the demo. But you could do something like this
  * to set up a monitor on a directory, so that when new files arrive, Spark streams
  * them in. New files are read as text files with 'textFileStream' (using key as LongWritable,
  * value as Text and input format as TextInputFormat)
  * {{{
  *   ssc.textFileStream(dir)
     .reduceByWindow(_ + _, Seconds(30), Seconds(1))
  * }}}
  */
class RawFeedActor(val kafkaConfig: KafkaConfig, ssc: StreamingContext, settings: WeatherSettings)
  extends KafkaProducer {
  import WeatherEvent._
  import settings._

  def receive : Actor.Receive = {
    case PublishFeed(years) => publish(years, sender)
  }

  /** TODO from s3 */
  def publish(years: Range, requestor: ActorRef): Unit =
    years foreach { n =>
      val location = s"$DataDirectory/200$n.csv.gz"
      val lines = ssc.sparkContext.textFile(location).flatMap(_.split("\\n")).toLocalIterator
      batchSend(KafkaTopicRaw, KafkaGroupId, KafkaBatchSendSize, lines.toSeq)
    }

}

/**
 * For a given weather station, calculates high, low and average temperature.
 * For the moment we do just the month interval.
 */
class TemperatureActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import com.datastax.spark.connector._
  import WeatherEvent._
  import settings._

  def receive : Actor.Receive = {
    case WeatherEvent.GetTemperature(sid, month, year) => compute(sid, month, year, sender)
  }

  /* The iterator will consume as much memory as the largest partition in this RDD */
  def compute(sid: String, month: Int, year: Int, requester: ActorRef): Unit = Future {
      val rdd = ssc.sparkContext.cassandraTable[Double](CassandraKeyspace, CassandraTableRaw)
                .select("temperature")
                .where("weather_station = ? AND month = ? AND year = ?", sid, month, year)

      Temperature(sid, rdd)
  } pipeTo requester

}

/**
 * For a given weather station, calculates annual cumulative precip - or year to date.
 */
class PrecipitationActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import com.datastax.spark.connector._
  import WeatherEvent._
  import settings._

  def receive : Actor.Receive = {
    case WeatherEvent.GetPrecipitation(sid, year) => compute(sid, year, sender)
  }

  def compute(sid: String, year: Int, requester: ActorRef): Unit = Future {
    val rdd = ssc.sparkContext.cassandraTable[Double](CassandraKeyspace, CassandraTableRaw)
              .select("oneHourPrecip")
              .where("id = ? AND year = ?", sid, year)

    Precipitation(sid, rdd)
  } pipeTo requester

}

/** For a given weather station id, retrieves the full station data. */
class WeatherStationActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import com.datastax.spark.connector._
  import settings._

  def receive : Actor.Receive = {
    case WeatherEvent.GetWeatherStation(sid) => weatherStation(sid, sender)
  }

  /** Fill out the where clause and what needs to be passed in to request one.
    *
    * The reason we can not allow a `LIMIT 1` in the `where` function is that
    * the query is executed on each node, so the limit would applied in each
    * query invocation. You would probably receive about partitions_number * limit results.
    */
  def weatherStation(sid: String, requester: ActorRef): Unit =
    for {
      stations <- ssc.sparkContext.cassandraTable[Weather.WeatherStation](CassandraKeyspace, CassandraTableStations)
                  .where("id = ?", sid)
                  .collectAsync
      station <- stations.headOption
    } requester ! station

}
