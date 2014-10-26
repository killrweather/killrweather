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

import com.typesafe.config.Config

/**
 * Application settings. First attempts to acquire from the deploy environment.
 * If not exists, then from -D java system properties, else a default config.
 *
 * @param conf Optional config for test
 */
final class WeatherSettings(conf: Option[Config] = None) extends Settings(conf) {

  val CassandraKeyspace = killrweather.getString("cassandra.keyspace")
  val CassandraTableRaw = killrweather.getString("cassandra.table.raw")
  val CassandraTableDailyTemp = killrweather.getString("cassandra.table.daily.temperature")
  val CassandraTableDailyPrecip = killrweather.getString("cassandra.table.daily.precipitation")
  val CassandraTableCumulativePrecip = killrweather.getString("cassandra.table.cumulative.precipitation")
  val CassandraTableSky = killrweather.getString("cassandra.table.sky")
  val CassandraTableStations = killrweather.getString("cassandra.table.stations")

  val SparkCheckpointDir = killrweather.getString("spark.checkpoint.dir")

  val DataLoadPath = killrweather.getString("data.load.path")

  val HistoricDataYearRange: Range = {
    val s = killrweather.getInt("data.raw.year.start")
    val e = killrweather.getInt("data.raw.year.end")
    s to e
  }

  import Weather.UriYearPartition
  val ByYearPartitions: Seq[UriYearPartition] =
    for (year <- HistoricDataYearRange) yield
      UriYearPartition(year, s"$DataLoadPath/$year.csv.gz")
}
