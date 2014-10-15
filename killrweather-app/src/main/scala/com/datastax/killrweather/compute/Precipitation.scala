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

import org.apache.spark.util.StatCounter

import scala.concurrent.Future
import scala.util.control.NonFatal
import akka.actor.{Props, Actor, ActorRef}
import akka.pattern.pipe
import akka.routing.RoundRobinRouter
import org.joda.time.DateTime
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.WeatherEvent
import com.datastax.killrweather.WeatherSettings
import com.datastax.killrweather.actor.WeatherActor

/** Supervisor for a given `year`. */
class PrecipitationSupervisor(year: Int, ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import WeatherEvent._

  val precipitation = context.actorOf(Props(new PrecipitationActor(ssc, settings)))

  /** Creates the daily precip router to parallelize the work. Just 1 for now.*/
  val dailyPrecipitation = context.actorOf(Props(
    new DailyPrecipitationActor(ssc, settings))
    .withRouter(RoundRobinRouter(nrOfInstances = 1,
    routerDispatcher = "killrweather.dispatchers.precipitation")))

  def receive : Actor.Receive = {
    case e: WeatherStationIds    => runTask(e.sids: _*)
    case e: GetPrecipitation     => precipitation forward e
    case e: GetTopKPrecipitation => precipitation forward e
  }

  /** Sends a [[ComputeDailyTemperature]] command, round robin, to the daily
    * temperature actors to compute for the given weather station id and year. */
  def runTask(sids: String*): Unit =
    sids.foreach(dailyPrecipitation ! ComputeDailyPrecipitation(_, year))

}

class DailyPrecipitationActor (ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings.{CassandraKeyspace => keyspace,
                   CassandraTableDailyPrecip => dailytable,
                   CassandraTableRaw => rawtable}
  import WeatherEvent._

  def receive : Actor.Receive = {
    case ComputeDailyPrecipitation(sid, year, cst) => compute(sid, year)// TODO(cst)
  }

  def compute(sid: String, year: Int): Unit = {
    val start = dayOfYearForYear(1, year)
    streamDays(start).take(366).filter(isValid(_, start)).toList.map(computeDay(_, sid))
  }

  /** For the given day of the year, aggregates all the precip values to statistics.
    * Persists to Cassandra daily precip table by weather station.
    */
  def computeDay(dt: DateTime, wsid: String): Unit = {
    log.debug(s"Computing ${toDateFormat(dt)} for $wsid")

    ssc.cassandraTable[(String, Double)](keyspace, rawtable)
      .select("weather_station", "precipitation")
      .where("weather_station = ? AND year = ? AND month = ? AND day = ?",
        wsid, dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth)
      .map { case (_, p) => DailyPrecipitation(wsid, dt, p) }
      .saveToCassandra(keyspace, dailytable)

    if (dt.getDayOfYear >= 365) publishStatus(wsid, dt.getYear)
  }

  def publishStatus(wsid: String, year: Int): Unit =
    context.system.eventStream.publish(DailyPrecipitationTaskCompleted(self, wsid, year))

}

/** 5. For a given weather station, calculates annual cumulative precip - or year to date. */
class PrecipitationActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableDailyPrecip => dailytable}
  import WeatherEvent._

 // implicit def ordering: Ordering[(String,Double)] = Ordering.by(_._2)

  def receive : Actor.Receive = {
    case GetPrecipitation(wsid, year) => cumulative(wsid, year, sender)
    case GetTopKPrecipitation(year)   => topK(year, sender)
  }

  /** Returns a future value to the `requester` actor.
    * Precipitation values are 1 hour deltas from the previous. */
  def cumulative(wsid: String, year: Int, requester: ActorRef): Unit = {
    log.debug(s"Computing $year for $wsid")

    doCumulative(wsid, year) pipeTo requester
  }

  /** Tutorial: find a better way to do this with spark than collectAsync. */
  def doCumulative(wsid: String, year: Int): Future[Precipitation] =
    ssc.cassandraTable[Double](keyspace, dailytable)
      .select("precipitation")
      .where("weather_station = ? AND year = ?", wsid, year)
      .collectAsync()
      .map(a => Precipitation(wsid, year, a.sum))

  /** Returns the 10 highest temps for any station in the `year`. */
  def topK(year: Int, requester: ActorRef): Unit = Future {
    val top = ssc.cassandraTable[(String,Double)](keyspace, dailytable)
      .select("precipitation")
      .where("year = ?", year)
      .top(10)

    TopKPrecipitation(top)
  } pipeTo requester

}

