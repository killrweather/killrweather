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

import scala.concurrent.duration._
import akka.actor._
import com.datastax.spark.connector._

class TemperatureActorSpec extends ActorSparkSpec {

  import WeatherEvent._
  import Weather._
  import settings._

  val temperature = system.actorOf(Props(new TemperatureActor(sc, settings)))

  start(clean = true)

  "TemperatureActor" must {
    "aggregate hourly wsid temperatures for a given day and year" in {
      temperature ! GetDailyTemperature(sample)
      expectMsgPF(timeout.duration) {
        case aggregate: DailyTemperature =>
          validate(Day(aggregate.wsid, aggregate.year, aggregate.month, aggregate.day))
      }
    }
    "handle no data available" in {
      temperature ! GetDailyTemperature(sample.copy(year = 2020))
      expectMsgPF(timeout.duration) {
        case na: NoDataAvailable =>
      }
    }
    s"asynchronously store DailyTemperature data in $CassandraTableDailyTemp" in {
      val tableData = sc.cassandraTable[DailyTemperature](CassandraKeyspace, CassandraTableDailyTemp)
        .where("wsid = ? AND year = ? AND month = ? AND day = ?",
          sample.wsid, sample.year, sample.month, sample.day)

      awaitCond(tableData.toLocalIterator.toSeq.headOption.nonEmpty, 10.seconds)
      val aggregate = tableData.toLocalIterator.toSeq.head
      validate(Day(aggregate.wsid, aggregate.year, aggregate.month, aggregate.day))
    }
    "compute daily temperature rollups per weather station to monthly statistics." in {
      temperature ! GetMonthlyHiLowTemperature(sample.wsid, sample.year, sample.month)
      expectMsgPF(timeout.duration) {
        case aggregate: MonthlyTemperature =>
          validate(Day(aggregate.wsid, aggregate.year, aggregate.month, sample.day))
      }
    }
  }

  def validate(aggregate: Day): Unit = {
    aggregate.wsid should be(sample.wsid)
    aggregate.year should be(sample.year)
    aggregate.month should be(sample.month)
    aggregate.day should be(sample.day)
  }
}