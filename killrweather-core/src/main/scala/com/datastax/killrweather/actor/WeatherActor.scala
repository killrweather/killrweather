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
package com.datastax.killrweather.actor

import scala.concurrent.duration._
import akka.actor.{ActorLogging, Actor}
import akka.util.Timeout
import org.joda.time.{DateTimeZone, DateTime}

/** A base actor for weather data computation. */
trait WeatherActor extends Actor with ActorLogging {

  implicit val timeout = Timeout(5.seconds)

  implicit val ctx = context.dispatcher

  /** Creates a timestamp for the current date time in UTC. */
  def now: DateTime = new DateTime(DateTimeZone.UTC)

  /** Creates a lazy date stream, where elements are only evaluated when they are needed. */
  def allDays(from: DateTime): Stream[DateTime] = from #:: allDays(from.plusDays(1))

  /** Creates timestamp for a given year and day of year. */
  def dayOfYearForYear(doy: Int, year: Int): DateTime = new DateTime(DateTimeZone.UTC)
    .withYear(year).withDayOfYear(doy)

  /** Creates timestamp for a given year and day of year. */
  def monthAndYear(month: Int, year: Int): DateTime = new DateTime(DateTimeZone.UTC)
    .withYear(year).withMonthOfYear(month)
}
