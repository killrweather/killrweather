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
package com.datastax.killrweather.clients

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor._
import org.apache.spark.streaming.StreamingContext
import org.joda.time.{DateTimeZone, DateTime}
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.KafkaConsumer
import com.datastax.killrweather.{Weather, WeatherEvent, WeatherSettings}
import com.datastax.spark.connector.embedded.{Assertions, ZookeeperConnectionString => zookeeper}

class KillrClient(settings: WeatherSettings, ssc: StreamingContext, guardian: ActorRef)
  extends Actor with ActorLogging with Assertions {

  import WeatherEvent._
  import Weather._
  import settings._
  import context.dispatcher

  var task = context.system.scheduler.schedule(20.seconds, 30.seconds) {
    self ! QueryTask
  }

  var last: Day = Day("725030:14732", 2008, 12, 31) // just the initial one

  override def preStart(): Unit = log.info("Starting.")

  override def postStop(): Unit = task.cancel()

  /** The API thus far: */
  def receive: Actor.Receive = {
    case e: GetCurrentWeather =>
      log.info(s"Received {}", e)
    case e: AnnualPrecipitation =>
      log.info(s"Received {}", e)
    case e: TopKPrecipitation =>
      log.info(s"Received {}", e)
    case e: DailyTemperature =>
      log.info(s"Received {}", e)
    case e: WeatherStation =>
      log.info(s"Received {}", e)
    case e: NoDataAvailable =>
      log.info(s"No data found for query {}: {}", e.query, e)
    case QueryTask => queries()
  }

  def queries(): Unit = {

    val previous = (d: Day) => d.day == last.day && d.month == last.month

    val sample = ssc.cassandraTable[Day](CassandraKeyspace, CassandraTableRaw)
      .select("wsid", "year", "month", "day")
      .toLocalIterator.toSeq.filterNot(previous).head

    log.info(s"Requesting the current weather for weather station ${sample.wsid}")
    // because we load from historic file data vs stream in the cloud for this sample app ;)
    val timstamp = new DateTime(DateTimeZone.UTC).withYear(sample.year)
      .withMonthOfYear(sample.month).withDayOfMonth(sample.day)
    guardian ! GetCurrentWeather(sample.wsid, Some(timstamp))

    log.info(s"Requesting annual precipitation for weather station ${sample.wsid} in year ${sample.year}")
    guardian ! GetPrecipitation(sample.wsid, sample.year)

    log.info(s"Requesting the current weather for weather station ${sample.wsid}")
    guardian ! GetTopKPrecipitation(sample.wsid, sample.year, k = 10)

    log.info(s"Requesting the current weather for weather station ${sample.wsid}")
    guardian ! GetDailyTemperature(sample)

    log.info(s"Requesting the current weather for weather station ${sample.wsid}")
    guardian ! GetMonthlyHiLowTemperature(sample.wsid, sample.year, sample.month)

    log.info(s"Requesting the current weather for weather station ${sample.wsid}")
    guardian ! GetWeatherStation(sample.wsid)

    last = sample
  }
}

class KafkaClient(settings: WeatherSettings, ssc: StreamingContext, guardian: ActorRef)
  extends Actor with ActorLogging {

  import WeatherEvent._
  import settings._
  import context.dispatcher

  val atomic = new AtomicInteger(0)

  val consumer = new KafkaConsumer(zookeeper, KafkaTopicRaw, KafkaGroupId, 1, 10, atomic)

  var task = context.system.scheduler.schedule(3.seconds, 3.seconds) {
    self ! QueryTask
  }

  override def postStop(): Unit = task.cancel

  def receive: Actor.Receive = {
    case QueryTask =>
      log.info(s"Kafka message count [${atomic.get}]")
  }
}

