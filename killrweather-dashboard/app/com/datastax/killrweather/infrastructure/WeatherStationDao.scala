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
package com.datastax.killrweather.infrastructure

import com.datastax.driver.core.{Row, Session}
import com.datastax.killrweather.Weather.{DailyPrecipitation, AnnualPrecipitation, WeatherStation}
import com.datastax.killrweather.{Weather, WeatherStationId}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}

//todo deal with DB errors
class WeatherStationDao(session: Session) {

  private implicit class ListenableFutureExtension[T](lf: ListenableFuture[T]) {
    def toPromise: Future[T] = {
      val p = Promise[T]()
      Futures.addCallback(lf, new FutureCallback[T] {
        def onFailure(t: Throwable): Unit = p failure t
        def onSuccess(result: T): Unit    = p success result
      })
      p.future
    }
  }

  private val getWeatherStationsPS = session.prepare("select * from weather_station")
  private val getWeatherStationPS = session.prepare("select * from weather_station where id = ?")
  private val getYearlyPrecipPS = session.prepare("select * from year_cumulative_precip where wsid = ? and year = ?")
  private val getDailyPrecipPS = session.prepare("select * from daily_aggregate_precip where wsid = ? and year = ? and month = ? and day = ?")
  private val getDailyPrecipLimitPS = session.prepare("select * from daily_aggregate_precip where wsid = ? limit ?")

  def retrieveWeatherStations() : Future[Seq[WeatherStation]] = {
    session.executeAsync(getWeatherStationsPS.bind()).toPromise
      .map(rows => rows.all().asScala)
      .map(rows => rows.map(rowToWeatherStation))
  }

  def retrieveWeatherStation(stationId: WeatherStationId) : Future[Option[WeatherStation]] = {
    session.executeAsync(getWeatherStationPS.bind(stationId.id)).toPromise
      .map(rows => Option(rows.one()))
      .map(_.map(rowToWeatherStation))
  }

  def retrieveYearlyPrecipitation(stationId: WeatherStationId, year: java.lang.Integer): Future[AnnualPrecipitation] = {
    session.executeAsync(getYearlyPrecipPS.bind(stationId.id, year)).toPromise
      .map(_.one())
      .map(row => AnnualPrecipitation(
        row.getString("wsid"),
        row.getInt("year"),
        row.getLong("precipitation")))
  }

  def retrieveDailyPrecipitation(stationId: WeatherStationId, year: java.lang.Integer, month: java.lang.Integer, day: java.lang.Integer): Future[Weather.DailyPrecipitation] = {
    session.executeAsync(getDailyPrecipPS.bind(stationId.id, year, month, day)).toPromise
      .map(_.one())
      .map(mapDailyPrecipitation)
  }

  def retrieveDailyPrecipitation(stationId: WeatherStationId, limit: java.lang.Integer) : Future[Seq[Weather.DailyPrecipitation]] = {
    session.executeAsync(getDailyPrecipLimitPS.bind(stationId.id, limit)).toPromise
      .map(rows => rows.all().asScala)
      .map(rows => rows.map(mapDailyPrecipitation))
  }

  private def mapDailyPrecipitation: (Row) => DailyPrecipitation = {
    row => DailyPrecipitation(
      row.getString("wsid"),
      row.getInt("year"),
      row.getInt("month"),
      row.getInt("day"),
      row.getLong("precipitation"))
  }

  private def rowToWeatherStation(row: Row): WeatherStation = {
    WeatherStation(
      row.getString("id"),
      row.getString("name"),
      row.getString("country_code"),
      row.getString("call_sign"),
      row.getDouble("lat"),
      row.getDouble("long"),
      row.getDouble("elevation"))
  }
}

