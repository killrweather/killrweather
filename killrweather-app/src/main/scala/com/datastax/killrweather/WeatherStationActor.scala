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

import akka.pattern.pipe
import akka.actor.{ActorLogging, Actor, ActorRef}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import org.joda.time.DateTime

/** For a given weather station id, retrieves the full station data. */
class WeatherStationActor(sc: SparkContext, settings: WeatherSettings)
  extends AggregationActor with ActorLogging {

  import settings.{CassandraKeyspace => keyspace }
  import settings.{CassandraTableRaw => rawtable}
  import settings.{CassandraTableStations => weatherstations}
  import WeatherEvent._
  import Weather._

  def receive : Actor.Receive = {
    case GetCurrentWeather(wsid, dt) => current(wsid, dt, sender)
    case GetWeatherStation(wsid)     => weatherStation(wsid, sender)
  }

  /** Computes and sends the current weather conditions for a given weather station,
    * based on UTC time, to the `requester`.
    */
  def current(wsid: String, dt: Option[DateTime], requester: ActorRef): Unit = {
    val day = Day(wsid, dt getOrElse timestamp)
    sc.cassandraTable[RawWeatherData](keyspace, rawtable)
      .where("wsid = ? AND year = ? AND month = ? AND day = ?",
        wsid, day.year, day.month, day.day)
      .collectAsync.map(_.headOption) pipeTo requester
  }

  /** The reason we can not allow a `LIMIT 1` in the `where` function is that
    * the query is executed on each node, so the limit would applied in each
    * query invocation. You would probably receive about partitions_number * limit results.
    */
  def weatherStation(wsid: String, requester: ActorRef): Unit =
    sc.cassandraTable[Weather.WeatherStation](keyspace, weatherstations)
      .where("id = ?", wsid)
      .collectAsync.map(_.headOption) pipeTo requester

}