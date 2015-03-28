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
package com.datastax.killrweather.service

import com.datastax.killrweather.Weather.{DailyPrecipitation, WeatherStation}
import com.datastax.killrweather.WeatherStationId
import com.datastax.killrweather.infrastructure.WeatherStationDao
import play.Logger
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

class WeatherService(weatherStationDao: WeatherStationDao) {

  private val DefaultLimit = 50

  def getWeatherStation(stationId: WeatherStationId): Future[Option[WeatherStationInfo]] = {
    val station: Future[Option[WeatherStation]] = weatherStationDao.retrieveWeatherStation(stationId)
    val precipitation: Future[Seq[DailyPrecipitation]] = weatherStationDao.retrieveDailyPrecipitation(stationId, DefaultLimit)

    station.flatMap {
      case Some(weatherStation) => precipitation.flatMap(precips => Future.successful(Some(
        WeatherStationInfo(weatherStation, precips.map(dp => DayPrecipitation(s"${dp.year}-${dp.month}-${dp.day}", dp.precipitation))))))
      case None =>
        Logger.info(s"Unable to find station with ID $stationId")
        Future.successful(None)
    }
  }
}
