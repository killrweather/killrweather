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
import akka.actor.{Actor, ActorRef}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.actor.WeatherActor


/** For a given weather station id, retrieves the full station data. */
class WeatherStationActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor {
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableStations => weatherstations}
  import WeatherEvent._

  def receive : Actor.Receive = {
    case GetWeatherStation(sid)  => weatherStation(sid, sender)
    case StreamWeatherStationIds => streamWeatherStationIds(sender)
    case GetWeatherStationIds    => weatherStationIds(sender)
  }

  /** 1. Streams weather station Ids to the daily computation actors.
    * Requires less heap memory and system load, but is slower than collectAsync below.
    * The iterator will consume as much memory as the largest partition in this RDD. */
  def streamWeatherStationIds(requester: ActorRef): Unit =
    ssc.cassandraTable[String](keyspace, weatherstations).select("id")
      .toLocalIterator foreach (id => requester ! WeatherStationIds(id))

  /** 1. Collects weather station Ids async, to the daily computation actors.
    * Requires more heap memory and system load, but is faster than toLocalIterator above. */
  def weatherStationIds(requester: ActorRef): Unit =
    for (stations <- ssc.cassandraTable[String](keyspace, weatherstations).select("id").collectAsync)
    yield requester ! WeatherStationIds(stations: _*)

  /** The reason we can not allow a `LIMIT 1` in the `where` function is that
    * the query is executed on each node, so the limit would applied in each
    * query invocation. You would probably receive about partitions_number * limit results.
    */
  def weatherStation(sid: String, requester: ActorRef): Unit =
    for {
      stations <- ssc.cassandraTable[Weather.WeatherStation](keyspace, weatherstations)
        .where("id = ?", sid)
        .collectAsync
      station <- stations.headOption
    } requester ! station

}