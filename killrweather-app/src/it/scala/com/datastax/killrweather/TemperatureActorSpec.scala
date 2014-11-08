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

  val year = 2005

  val sid = "010010:99999"

  val temperature = system.actorOf(Props(new TemperatureActor(sc, settings)))

  "TemperatureActor" must {
    start()
     s"aggregate hourly wsid temperatures for a given day and year and asynchronously store in $CassandraTableDailyTemp" in {
       temperature ! GetDailyTemperature(sid, year, 1, 10)
       val aggregate = expectMsgPF[DailyTemperature](timeout.duration) {
         case Some(e) => e.asInstanceOf[DailyTemperature]
       }

       val tableData = sc.cassandraTable[DailyTemperature](CassandraKeyspace, CassandraTableDailyTemp)
         .where("wsid = ? AND year = ? AND month = ? AND day = ?",
           aggregate.wsid, aggregate.year, aggregate.month, aggregate.day)
       awaitCond(tableData.toLocalIterator.toSeq.headOption.nonEmpty, 10.seconds)
       println(tableData.toLocalIterator.toSeq.headOption)
    }
    "compute daily temperature rollups per weather station to monthly statistics." in {
      temperature ! GetMonthlyTemperature(sid, year, 1)
      val aggregate = expectMsgPF(timeout.duration) {
        case Some(e) => e.asInstanceOf[MonthlyTemperature]
      }
      aggregate.wsid should be (sid)
      aggregate.year should be (year)
      aggregate.month should be (1)
      println(s"For month: low=${aggregate.low} high=${aggregate.high}")
    }
  }
}