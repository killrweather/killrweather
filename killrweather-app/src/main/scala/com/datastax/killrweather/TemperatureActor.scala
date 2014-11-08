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

import org.apache.spark.SparkContext
import org.apache.spark.util.StatCounter

import scala.concurrent.Future
import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.pattern.pipe
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._

/** The TemperatureActor reads the daily temperature rollup data from Cassandra,
  * and for a given weather station, computes temperature statistics by month for a given year.
  */
class TemperatureActor(sc: SparkContext, settings: WeatherSettings)
  extends WeatherActor with ActorLogging {

  import settings.{CassandraKeyspace => keyspace, CassandraTableDailyTemp => dailytable, CassandraTableRaw => rawtable }
  import WeatherEvent._
  import Weather._

  def receive : Actor.Receive = {
    case e: GetDailyTemperature   => daily(e, sender)
    case e: DailyTemperature      => store(e)
    case e: GetMonthlyTemperature => monthly(e, sender)
  }

  /** Returns a future value to the `requester` actor.
    * We aggregate this data on-demand versus in the stream.
    *
    * For the given day of the year, aggregates 0 - 23 temp values to statistics:
    * high, low, mean, std, etc., and persists to Cassandra daily temperature table
    * by weather station, automatically sorted by most recent - due to our cassandra schema -
    * you don't need to do a sort in spark.
    *
    * Because the gov. data is not by interval (window/slide) but by specific date/time
    * we look for historic data for hours 0-23 that may or may not already exist yet
    * and create stats on does exist at the time of request.
    */
  def daily(e: GetDailyTemperature, requester: ActorRef): Future[Option[DailyTemperature]] =
    (for {
      a <- sc.cassandraTable[Double](keyspace, rawtable)
              .select("temperature").where("wsid = ? AND year = ? AND month = ? AND day = ?",
                 e.wsid, e.year, e.month, e.day)
              .collectAsync
    } yield forDay(DayKey(e.wsid, e.year, e.month, e.day), a)) pipeTo requester

  /** Stores the daily temperature aggregates asynchronously which are triggered
    * by on-demand requests during the `forDay` function's `self ! data`
    * to the daily temperature aggregation table. */
  def store(e: DailyTemperature): Unit =
    sc.makeRDD(Seq(e)).saveToCassandra(keyspace, dailytable)

  /** Returns a future value to the `requester` actor. */
  def monthly(e: GetMonthlyTemperature, requester: ActorRef): Future[Option[MonthlyTemperature]] =
    (for {
      a <- sc.cassandraTable[DailyTemperature](keyspace, dailytable)
              .where("wsid = ? AND year = ? AND month = ?", e.wsid, e.year, e.month)
              .collectAsync
    } yield forMonth(e.wsid, e.year, e.month, a)) pipeTo requester

  def forDay(key: DayKey, temps: Seq[Double]): Option[DailyTemperature] =
    if (temps.nonEmpty) {
      val aggregate = sc.parallelize(temps)
        .map { case temp => StatCounter(Seq(temp)) }
        .reduce(_ merge _) // would only ever be 0-23 small items

      val data = DailyTemperature(key, aggregate)
      self ! data
      Some(data)
    } else None

  def forMonth(wsid: String, year: Int, month: Int, temps: Seq[DailyTemperature]): Option[MonthlyTemperature] =
    if (temps.nonEmpty) {
      val rdd = sc.parallelize(temps)
      val high = rdd.map(_.high).max
      val low = rdd.map(_.low).min

      Some(MonthlyTemperature(wsid, year, month, high, low))
    } else None
}
