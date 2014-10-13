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

import akka.pattern.{ pipe, ask }
import akka.actor.{Props, Actor, ActorRef}
import akka.routing.RoundRobinRouter
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.joda.time.DateTime
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.actor.WeatherActor

/** Supervisor for a given `year`. */
class TemperatureSupervisor(year: Int, ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import WeatherEvent._

  /** Creates the daily temperature router, with `sids`.size / 2 number of [[DailyTemperatureActor]] instances. */
  val dailyTemperatures = context.actorOf(Props(
    new DailyTemperatureActor(ssc, settings))
    .withRouter(RoundRobinRouter(nrOfInstances = 1, // TODO weatherStationIds.size / 2
    routerDispatcher = "killrweather.dispatchers.temperature")))

  val temperature = context.actorOf(Props(new TemperatureActor(ssc, settings)))

  def receive : Actor.Receive = {
    case e: WeatherStationIds     => runTask(e.sids: _*)
    case e: GetMonthlyTemperature => temperature forward e
  }

  /** Sends a [[ComputeDailyTemperature]] command, round robin, to the daily
    * temperature actors to compute for the given weather station id and year. */
  def runTask(sids: String*): Unit =
    sids.foreach(dailyTemperatures ! ComputeDailyTemperature(_, year))

}

/** 4. The DailyTemperatureActor performs a background task that computes and persists daily
  * temperature statistics by weather station for a given year, and stores in
  * the daily temp rollup table in Cassandra for later computation.
  */
class DailyTemperatureActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor { 
  import WeatherEvent._
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableRaw => rawtable}
  import settings.{CassandraTableDailyTemp => dailytable}

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
    log.debug(s"Computing ${toDateFormat(dt)}")
    for {
      aggregate <- ssc.cassandraTable[Double](keyspace, rawtable)
                    .select("temperature")
                    .where("weather_station = ? AND year = ? AND month = ? AND day = ?",
                      wsid, dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth)
                    .collectAsync()
                    .map(DailyTemperature(wsid, dt, _))
    } yield {
      ssc.sparkContext.parallelize(Seq(aggregate)).saveToCassandra(keyspace, dailytable)
      if (dt.getDayOfYear >= 365) publishStatus(dt.getYear)
    }
  }

  def publishStatus(year: Int): Unit =
    context.system.eventStream.publish(DailyTemperatureTaskCompleted(self, year))
}

/** 5. The TemperatureActor reads the daily temperature rollup data from Cassandra,
  * and for a given weather station, computes temperature statistics by month for a given year.
  */
class TemperatureActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import WeatherEvent._
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableDailyTemp => dailytable}

  def receive : Actor.Receive = {
    case GetMonthlyTemperature(sid, doy, year) => compute(sid, doy, year, sender)
  }

  /** Returns a future value to the `requester` actor. */
  def compute(sid: String, doy: Int, year: Int, requester: ActorRef): Unit = {
    val dt = dayOfYearForYear(doy, year)
    for {
      temps <- ssc.cassandraTable[Double](keyspace, dailytable)
        .select("temperature")
        .where("weather_station = ? AND year = ? AND month = ? AND day = ?",sid, dt.year, dt.monthOfYear, dt.dayOfYear)
        .collectAsync
    } yield Temperature(sid, temps)
  } pipeTo requester

}