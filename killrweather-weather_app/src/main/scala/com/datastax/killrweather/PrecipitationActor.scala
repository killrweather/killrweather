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

import akka.actor.{ActorLogging, Actor, ActorRef}
import akka.pattern.pipe
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.streaming._

/** For a given weather station, calculates annual cumulative precip - or year to date. */
class PrecipitationActor(ssc: StreamingContext, settings: WeatherSettings)
  extends AggregationActor with ActorLogging {
  import Weather._
  import WeatherEvent._
  import settings.{CassandraKeyspace => keyspace}
  import settings.{CassandraTableDailyPrecip => dailytable}

  def receive : Actor.Receive = {
    case GetPrecipitation(wsid, year)        => cumulative(wsid, year, sender)
    case GetTopKPrecipitation(wsid, year, k) => topK(wsid, year, k, sender)
  }

  /** Computes and sends the annual aggregation to the `requester` actor.
    * Precipitation values are 1 hour deltas from the previous. */
  def cumulative(wsid: String, year: Int, requester: ActorRef): Unit =
    ssc.cassandraTable[Double](keyspace, dailytable)
      .select("precipitation")
      .where("wsid = ? AND year = ?", wsid, year)
      .collectAsync()
      .map(AnnualPrecipitation(_, wsid, year)) pipeTo requester

  /** Returns the 10 highest temps for any station in the `year`. */
  def topK(wsid: String, year: Int, k: Int, requester: ActorRef): Unit = {
    val toTopK = (aggregate: Seq[Double]) => TopKPrecipitation(wsid, year,
      ssc.sparkContext.parallelize(aggregate).top(k).toSeq)

    ssc.cassandraTable[Double](keyspace, dailytable)
      .select("precipitation")
      .where("wsid = ? AND year = ?", wsid, year)
      .collectAsync().map(toTopK) pipeTo requester
  }
}

