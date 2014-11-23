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

import akka.actor._
import org.joda.time.{DateTime, DateTimeZone}

import scala.concurrent.duration._

class WeatherApiQueries(settings: WeatherSettings, actor: ActorSelection)
  extends AggregationActor with ActorLogging {

  import com.datastax.killrweather.Weather._
  import com.datastax.killrweather.WeatherEvent._
  import settings._

  var task = context.system.scheduler.schedule(3.seconds, 2.seconds) {
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

    val previous = (d: Day) => queried contains d

    val sample: Day = (for {
      file <- IngestionData
      data <- getLines(file).map(Day(_)).filterNot(previous)
    } yield data).head

    log.info("Requesting the current weather for weather station {}", sample.wsid)
    // because we load from historic file data vs stream in the cloud for this sample app ;)
    val timestamp = new DateTime(DateTimeZone.UTC).withYear(sample.year)
      .withMonthOfYear(sample.month).withDayOfMonth(sample.day)
    actor ! GetCurrentWeather(sample.wsid, Some(timestamp))

    log.info("Requesting annual precipitation for weather station {} in year {}", sample.wsid, sample.year)
    actor ! GetPrecipitation(sample.wsid, sample.year)

    log.info("Requesting top-k Precipitation for weather station {}", sample.wsid)
    actor ! GetTopKPrecipitation(sample.wsid, sample.year, k = 10)

    log.info("Requesting the daily temperature aggregate for weather station {}", sample.wsid)
    actor ! GetDailyTemperature(sample)

    log.info("Requesting the high-low temperature aggregate for weather station {}",sample.wsid)
    actor ! GetMonthlyHiLowTemperature(sample.wsid, sample.year, sample.month)

    log.info("Requesting weather station {}", sample.wsid)
    actor ! GetWeatherStation(sample.wsid)

    queried += sample
  }

  // slinking away in shame for writing this..
  protected def getLines(file: String): Seq[String] = {
    import java.io.{BufferedInputStream, FileInputStream}
    import java.util.zip.GZIPInputStream

    val a = new FileInputStream(file)
    val b = new BufferedInputStream(a)
    val c = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))
    var samples: Seq[String] = Seq.empty
    try {
      val stream = scala.io.Source.fromInputStream(c)
      samples ++= stream.getLines.toList
      stream.close()
    } finally a.close();b.close;c.close;

    samples
  }
}
