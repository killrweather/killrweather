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

import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.Try
import akka.cluster.Cluster
import akka.actor._
import org.joda.time.{DateTime, DateTimeZone}

object KillrWeatherClientApp extends App with ClientHelper {

  val system = ActorSystem("KillrWeather")

  val cluster = Cluster(system)
  cluster.joinSeedNodes(immutable.Seq(cluster.selfAddress))

  /** Drives demo activity by sending requests to the NodeGuardian actor. */
  val queryClient = system.actorOf(Props[WeatherApiQueries], "api-client")

}

private[killrweather] class WeatherApiQueries extends Actor with ActorLogging with ClientHelper {

  import Weather._
  import WeatherEvent._
  import DataSourceEvent._
  import context.dispatcher

  val guardian = context.actorSelection(Cluster(context.system).selfAddress.copy(port = Some(BasePort)) + "/user/node-guardian")

  val task = context.system.scheduler.schedule(3.seconds, 2.seconds) {
    self ! QueryTask
  }

  var queried: Set[Day] = Set(Day("725030:14732", 2008, 12, 31)) // just the initial one

  override def preStart(): Unit = log.info("Starting.")

  override def postStop(): Unit = task.cancel()

  def receive: Actor.Receive = {
    case e: WeatherAggregate =>
      log.info("Received {} from {}", e, sender)
    case e: WeatherModel =>
      log.info("Received {} from {}", e, sender)
    case QueryTask => queries()
  }

  def queries(): Unit = {

    val previous = (day: Day) => {
      val key = day.wsid.split(":")(0)
      queried.exists(_.wsid.startsWith(key))
    }

    val day: Option[Day] = initialData.flatMap { case f =>
      val source = FileSource(f).source
      val s = source.getLines().map(Day(_)).filterNot(previous).toSeq.headOption
      Try(source.close())
      s
    }.headOption

    for (sample <- day) {
      log.info("Requesting the current weather for weather station {}", sample.wsid)
      // because we load from historic file data vs stream in the cloud for this sample app ;)
      val timestamp = new DateTime(DateTimeZone.UTC).withYear(sample.year)
        .withMonthOfYear(sample.month).withDayOfMonth(sample.day)
      guardian ! GetCurrentWeather(sample.wsid, Some(timestamp))

      log.info("Requesting annual precipitation for weather station {} in year {}", sample.wsid, sample.year)
      guardian ! GetPrecipitation(sample.wsid, sample.year)

      log.info("Requesting top-k Precipitation for weather station {}", sample.wsid)
      guardian ! GetTopKPrecipitation(sample.wsid, sample.year, k = 10)

      log.info("Requesting the daily temperature aggregate for weather station {}", sample.wsid)
      guardian ! GetDailyTemperature(sample)

      log.info("Requesting the high-low temperature aggregate for weather station {}",sample.wsid)
      guardian ! GetMonthlyHiLowTemperature(sample.wsid, sample.year, sample.month)

      log.info("Requesting weather station {}", sample.wsid)
      guardian ! GetWeatherStation(sample.wsid)

      queried += sample
    }
  }
}
