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

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.joda.time.DateTime

object Weather {

  /** Base marker trait. */
  sealed trait WeatherDataMarker extends Serializable
  trait DataRequest extends WeatherDataMarker
  trait DataResponse extends WeatherDataMarker

  /** Keeping as flat as possible for now, for simplicity. May modify later when time. */
  trait WeatherModel extends WeatherDataMarker

  /**
   * @param id Composite of Air Force Datsav3 station number and NCDC WBAN number
   * @param name Name of reporting station
   * @param countryCode 2 letter ISO Country ID // TODO restrict
   * @param callSign International station call sign
   * @param lat Latitude in decimal degrees
   * @param long Longitude in decimal degrees
   * @param elevation Elevation in meters
   */
  case class WeatherStation(
    id: String,
    name: String,
    countryCode: String,
    callSign: String,
    lat: Float,
    long: Float,
    elevation: Float) extends WeatherModel

  /**
   * @param weatherStation Composite of Air Force Datsav3 station number and NCDC WBAN number
   * @param year Year collected
   * @param month Month collected
   * @param day Day collected
   * @param hour Hour collected
   * @param temperature Air temperature (degrees Celsius)
   * @param dewpoint Dew point temperature (degrees Celsius)
   * @param pressure Sea level pressure (hectopascals)
   * @param windDirection Wind direction in degrees. 0-359
   * @param windSpeed Wind speed (meters per second)
   * @param skyCondition Total cloud cover (coded, see format documentation)
   * @param skyConditionText Non-coded sky conditions
   * @param oneHourPrecip One-hour accumulated liquid precipitation (millimeters)
   * @param sixHourPrecip Six-hour accumulated liquid precipitation (millimeters)
   */
  case class RawWeatherData(
    weatherStation: String,
    year: Int,
    month: Int,
    day: Int,
    hour: Int,
    temperature: Float,
    dewpoint: Float,
    pressure: Float,
    windDirection: Int,
    windSpeed: Float,
    skyCondition: Int,
    skyConditionText: String,
    oneHourPrecip: Float,
    sixHourPrecip: Float) extends WeatherModel

  object RawWeatherData {
    /** Tech debt - don't do it this way ;) */
    def apply(array: Array[String]): RawWeatherData = {
      RawWeatherData(
        weatherStation = array(0),
        year = array(1).toInt,
        month = array(2).toInt,
        day = array(3).toInt,
        hour = array(4).toInt,
        temperature = array(5).toFloat,
        dewpoint = array(6).toFloat,
        pressure = array(7).toFloat,
        windDirection = array(8).toInt,
        windSpeed = array(9).toFloat,
        skyCondition = array(10).toInt,
        skyConditionText = array(11),
        oneHourPrecip = array(11).toFloat,
        sixHourPrecip = Option(array(12).toFloat).getOrElse(0))
    }
  }

  trait WeatherAggregate extends WeatherModel

  case class Temperature(sid: String, year: Int, month: Int, day: Int, hour: Int, temperature: Float) extends WeatherModel

  case class TemperatureAggregate(sid: String, high: Double, low: Double, mean: Double, variance: Double, stdev: Double) extends WeatherAggregate
  object TemperatureAggregate {
    def apply(id: String, rdd: RDD[Double]): TemperatureAggregate =
      TemperatureAggregate(
        sid = id, high = rdd.max, low = rdd.min, mean = rdd.mean, variance = rdd.variance, stdev = rdd.stdev)
  }

  case class Precipitation(sid: String, year: Int, month: Int, day: Int, hour: Int, oneHourPrecip: Float) extends WeatherModel

  case class PrecipitationAggregate(sid: String, annual: Double) extends WeatherAggregate
}
