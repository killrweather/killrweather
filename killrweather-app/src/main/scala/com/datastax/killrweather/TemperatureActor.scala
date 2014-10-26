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

/** The TemperatureActor reads the daily temperature rollup data from Cassandra,
  * and for a given weather station, computes temperature statistics by month for a given year.
  */
class TemperatureActor(ssc: StreamingContext, settings: WeatherSettings)
  extends WeatherActor with ActorLogging {
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