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
import scala.concurrent.duration._
import akka.pattern.{ pipe, ask }
import akka.actor.{Props, Actor, ActorRef}
import akka.routing.RoundRobinRouter
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import org.joda.time.DateTime
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.api.WeatherApi
import com.datastax.killrweather.actor.WeatherActor

import WeatherEvent._

/** Supervisor for a given `year`. */
class TemperatureSupervisor(year: Int, ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import WeatherApi.WeatherStationId

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
  import com.datastax.killrweather.syntax.future._
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableRaw => rawtable}
  import settings.{CassandraTableDailyTemp => dailytable}

  def receive : Actor.Receive = {
    case ComputeDailyTemperature(sid, year) => compute(sid, year)
  }

  /* Compute aggregate for the given year, starting January 1, and store in daily rollup table. */
  def compute(sid: String, year: Int): Unit =
    for (aggregate <- computeYear(sid, dayOfYearForYear(1, year))) yield {
      ssc.sparkContext.parallelize(aggregate).saveToCassandra(keyspace, dailytable)
      context stop self
    }

  /* Streams only the valid days for the given year. */
  def computeYear(sid: String, start: DateTime): Future[Seq[Temperature]] =
    Future.sequence(for {
      day  <- allDays(start).filter(_.getYear == start.getYear).filter(_ isBeforeNow)//.toList
    } yield computeDay(day, sid).run.valueOrThrow)

  /** For the given day of the year, aggregates all the temp values to statistics: high, low, mean, std, etc.
    * Persists to Cassandra daily temperature table by weather station. */
  def computeDay(dt: DateTime, sid: String): FutureT[Temperature] =
    (for {
      values <- ssc.cassandraTable[Double](keyspace, rawtable)
                .select("temperature")
                .where("weather_station = ? AND year = ? AND month = ? AND day = ?",
                  sid, dt.getYear, dt.getMonthOfYear, dt.getDayOfYear)
                .collectAsync
    } yield Temperature(sid, values)).eitherT
}

/** 5. The TemperatureActor reads the daily temperature rollup data from Cassandra,
  * and for a given weather station, computes temperature statistics by month for a given year.
  */
class TemperatureActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
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