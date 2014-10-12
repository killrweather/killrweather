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

import org.apache.spark.util.StatCounter

object WeatherEvent {
  import org.apache.spark.rdd.RDD
  import org.apache.spark.SparkContext._

  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait WeatherEvent extends Serializable

  sealed trait LifeCycleEvent extends WeatherEvent
  case class PublishFeed(years: Range) extends LifeCycleEvent
  case object Shutdown extends LifeCycleEvent
  case object TaskCompleted extends LifeCycleEvent
  case object StartValidation extends LifeCycleEvent
  case object ValidationCompleted extends LifeCycleEvent

  sealed trait WeatherRequest extends WeatherEvent
  sealed trait WeatherResponse extends WeatherEvent
  trait WeatherAggregate extends WeatherResponse

  /** Composite of Air Force Datsav3 station number and NCDC WBAN number
    * @param sid uses the composite key format: stationNum:wbanNum
    */
  case class GetWeatherStation(sid: String) extends WeatherRequest
  case class GetRawWeatherData(perPage: Int) extends WeatherRequest

  case object GetWeatherStationIds extends WeatherRequest
  case object StreamWeatherStationIds extends WeatherRequest
  case class WeatherStationIds(sids: String*) extends WeatherResponse  

  case class ComputeDailyTemperature(sid: String, year: Int) extends WeatherRequest
  case class GetTemperature(sid: String, doy: Int, year: Int) extends WeatherRequest
  case class GetMonthlyTemperature(sid: String, doy: Int, year: Int) extends WeatherRequest
  case class Temperature(sid: String, high: Double, low: Double, mean: Double, variance: Double, stdev: Double) extends WeatherAggregate
  object Temperature {
    def apply(id: String, values: Seq[Double]): Temperature = {
      val s = StatCounter(values)
      Temperature(sid = id, high = s.max, low = s.min, mean = s.mean, variance = s.variance, stdev = s.stdev)
    }
  }

  case class GetPrecipitation(sid: String, year: Int) extends WeatherRequest
  case class Precipitation(sid: String, annual: Double) extends WeatherAggregate
   object Precipitation {
    def apply(sid: String, values: Seq[Double]): Precipitation = {
      val s = StatCounter(values)
      Precipitation(sid, s.sum)
    }
  }
  case class GetTopKPrecipitation(year: Int) extends WeatherRequest
  case class TopKPrecipitation(seq: Seq[(String, Double)]) extends WeatherAggregate

  /**
   * Quick access lookup table for sky_condition. Useful for potential analytics.
   * See http://en.wikipedia.org/wiki/Okta
   */
  case class GetSkyConditionLookup(code: Int) extends WeatherRequest

}

