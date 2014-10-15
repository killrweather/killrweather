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
package com.datastax.killrweather.compute

import scala.concurrent.Future
import scala.util.control.NonFatal
import akka.actor.{Actor, ActorRef, Props}
import akka.pattern.pipe
import akka.routing.RoundRobinRouter
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.util.StatCounter
import org.joda.time.DateTime
import com.datastax.killrweather.WeatherSettings
import com.datastax.killrweather.actor.WeatherActor
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

/** Supervisor for a given `year`. */
class TemperatureSupervisor(year: Int, ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import com.datastax.killrweather.WeatherEvent._

  val temperature = context.actorOf(Props(new TemperatureActor(ssc, settings)))

  /** Creates the daily temperature router to parallelize the work. Just 1 for now.*/
  val dailyTemperatures = context.actorOf(Props(
    new DailyTemperatureActor(ssc, settings))
    .withRouter(RoundRobinRouter(nrOfInstances = 1,
    routerDispatcher = "killrweather.dispatchers.temperature")))

  def receive : Actor.Receive = {
    case e: WeatherStationIds     => runTask(e.sids: _*)
    case e: GetMonthlyTemperature => temperature forward e
  }

  /** Sends a [[ComputeDailyTemperature]] command, round robin, to the daily
    * temperature actors to compute for the given weather station id and year. */
  def runTask(sids: String*): Unit = {
    //val values = sids.take(2) // Note - take just to run quickly for demo
    sids.foreach(dailyTemperatures ! ComputeDailyTemperature(_, year))
  } // TODO need to keep this data updated

}

/** 4. The DailyTemperatureActor performs a background task that computes and persists daily
  * temperature statistics by weather station for a given year, and stores in
  * the daily temp rollup table in Cassandra for later computation.
  */
class DailyTemperatureActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings.{CassandraKeyspace => keyspace, CassandraTableDailyTemp => dailytable, CassandraTableRaw => rawtable}
  import com.datastax.killrweather.WeatherEvent._

  def receive : Actor.Receive = {
    case ComputeDailyTemperature(sid, year, cst) => compute(sid, year)(cst)
  }

  /** Compute aggregate for the given year, starting January 1, and store in daily rollup table.
    * @param sid the weather station id
    * @param year the year for annual computation
    * @param constraint an optional constraint for testing. If it is not set,
    *                   computation starts at the default: January 1 of `year`.
    */
  def compute(sid: String, year: Int)(constraint: Option[Int]): Unit =
    computeYear(sid, start = dayOfYearForYear(constraint.getOrElse(1), year))

  /* Streams only the valid days for the given year, including a leap year. */
  def computeYear(sid: String, start: DateTime): Unit =
    streamDays(start).take(366).filter(isValid(_, start))
      .toList.map(computeDay(_, sid))

  /** For the given day of the year, aggregates all the temp values to statistics: high, low, mean, std, etc.
    * Persists to Cassandra daily temperature table by weather station.
    */
  def computeDay(dt: DateTime, wsid: String): Unit = {
    log.debug(s"Computing ${toDateFormat(dt)} for $wsid")

    val toDailyTemperature = (s: StatCounter) => DailyTemperature(wsid, dt, s)
 
    try
      ssc.cassandraTable[(String, Double)](keyspace, rawtable)
      .select("weather_station", "temperature")
      .where("weather_station = ? AND year = ? AND month = ? AND day = ?",
        wsid, dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth)
      .mapValues{ t => StatCounter(Seq(t))}
      .reduceByKey(_ merge _)
      .map { case (_, s) => toDailyTemperature(s) }
      .saveToCassandra(keyspace, dailytable)
    catch {
      case NonFatal(e) =>
        log.error(e,
          s"""
             Error processing data for station '$wsid' on '$dt'. You may just need to load all the raw data.
             Or this may be happening on IT test shutdown because it hasn't been done cleanly yet :)
           """)

    }

    if (dt.getDayOfYear >= 365) publishStatus(wsid, dt.getYear)
  }

  def publishStatus(wsid: String, year: Int): Unit = {
    log.info(s"Completed $year for $wsid")
    context.system.eventStream.publish(DailyTemperatureTaskCompleted(self, wsid, year))
  }
}

/** 5. The TemperatureActor reads the daily temperature rollup data from Cassandra,
  * and for a given weather station, computes temperature statistics by month for a given year.
  */
class TemperatureActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import com.datastax.killrweather.WeatherEvent._
  import settings.{CassandraKeyspace => keyspace, CassandraTableDailyTemp => dailytable}

  def receive : Actor.Receive = {
    case GetMonthlyTemperature(wsid, month, year) => monthly(wsid, month, year, sender)
    case GetTemperature(wsid, doy, year) => ???
  }

  /** Returns a future value to the `requester` actor. */
  def monthly(wsid: String, month: Int, year: Int, requester: ActorRef): Unit =
    doMonthly(wsid, month, year) pipeTo requester

  def doMonthly(wsid: String, year: Int, month: Int): Future[Seq[Temperature]] =
    ssc.cassandraTable[Temperature](keyspace, dailytable)
      .where("weather_station = ? AND year = ? AND month = ?",
        wsid, year, month)
      .collectAsync

}