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
import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.pattern.pipe
import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext

/** For a given weather station, calculates annual cumulative precip - or year to date. */
class PrecipitationActor(ssc: StreamingContext, settings: WeatherSettings)
  extends WeatherActor with ActorLogging {
  import Weather._
  import WeatherEvent._
  import settings.{CassandraKeyspace => keyspace, CassandraTableDailyPrecip => dailytable}

  implicit def ordering: Ordering[(String,Double)] = Ordering.by(_._2)

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

