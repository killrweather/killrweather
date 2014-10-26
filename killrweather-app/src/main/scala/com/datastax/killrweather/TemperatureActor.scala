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

  import settings.{CassandraKeyspace => keyspace, CassandraTableDailyTemp => dailytable}
  import WeatherEvent._
  import Weather._


  def receive : Actor.Receive = {
    case e: GetDailyTemperature   => daily(e, sender)
    case e: GetMonthlyTemperature => monthly(e, sender)
  }

  /** Returns a future value to the `requester` actor. */
  def daily(e: GetDailyTemperature, requester: ActorRef): Future[Option[DailyTemperature]] =
    (for {
      a <- ssc.cassandraTable[DailyTemperature](keyspace, dailytable)
        .where("wsid = ? AND year = ? AND month = ?", e.wsid, e.year, e.month, e.day)
        .collectAsync
    } yield a.headOption) pipeTo requester

  /** Returns a future value to the `requester` actor. */
  def monthly(e: GetMonthlyTemperature, requester: ActorRef): Future[Option[MonthlyTemperature]] =
    (for {
      a <- ssc.cassandraTable[DailyTemperature](keyspace, dailytable)
        .where("wsid = ? AND year = ? AND month = ?", e.wsid, e.year, e.month)
        .collectAsync
    } yield forMonth(e.wsid, e.year, e.month, a)) pipeTo requester

  def forMonth(wsid: String, year: Int, month: Int, temps: Seq[DailyTemperature]): Option[MonthlyTemperature] =
    if (temps.nonEmpty) {
      val rdd = ssc.sparkContext.parallelize(temps)
      val high = rdd.map(_.high).max
      val low = rdd.map(_.low).min

      Some(MonthlyTemperature(wsid, year, month, high, low))
    } else None
}
