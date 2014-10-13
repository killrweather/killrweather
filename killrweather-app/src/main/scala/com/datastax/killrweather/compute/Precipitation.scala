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
import akka.actor.{Props, Actor, ActorRef}
import akka.pattern.pipe
import akka.routing.RoundRobinRouter
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.WeatherEvent._
import com.datastax.killrweather.WeatherSettings
import com.datastax.killrweather.actor.WeatherActor

/** Supervisor for a given `year`. */
class PrecipitationSupervisor(year: Int, ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import com.datastax.killrweather.WeatherEvent._

  /** Creates the daily precip router, with `sids`.size / 2 number of [[DailyTemperatureActor]] instances. */
  val dailyPrecipitation = context.actorOf(Props(
    new DailyPrecipitationActor(ssc, settings))
    .withRouter(RoundRobinRouter(nrOfInstances = 1, // TODO weatherStationIds.size / 2
    routerDispatcher = "killrweather.dispatchers.temperature")))

  val precipitation = context.actorOf(Props(new PrecipitationActor(ssc, settings)))

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
  import settings.{CassandraKeyspace => keyspace, CassandraTableDailyPrecip => dailytable, CassandraTableRaw => rawtable}
  import com.datastax.killrweather.WeatherEvent._

  def receive : Actor.Receive = {
    case ComputeDailyPrecipitation(sid, year, cst) => //compute(sid, year)(cst)
  }

}

/** 5. For a given weather station, calculates annual cumulative precip - or year to date. */
class PrecipitationActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableDailyPrecip => dailytable}

  implicit def ordering: Ordering[(String,Double)] = Ordering.by(_._2)

  def receive : Actor.Receive = {
    case GetPrecipitation(wsid, year) => compute(wsid, year, sender)
    case GetTopKPrecipitation(year)  => topK(year, sender)
  }

  /** Returns a future value to the `requester` actor.
    * Precipitation values are 1 hour deltas from the previous. */
  def compute(wsid: String, year: Int, requester: ActorRef): Unit = {
    val dt = timestamp.withYear(year)

    def toPrecipitation(values: Seq[Double]): Precipitation = {
      val s = toStatCounter(values)
      Precipitation(wsid, s.sum)
    }

    for {
      precip <- ssc.cassandraTable[Double](keyspace, dailytable)
        .select("precipitation")
        .where("weather_station = ? AND year = ?", wsid, year)
        .collectAsync
        .map(toPrecipitation)
    } yield precip

  } pipeTo requester

  /** Returns the 10 highest temps for any station in the `year`. */
  def topK(year: Int, requester: ActorRef): Unit = Future {
    val top = ssc.cassandraTable[(String,Double)](keyspace, dailytable)
      .select("weather_station","precipitation")
      .where("year = ?", year)
      .top(10)

    TopKPrecipitation(top)
  } pipeTo requester

}

