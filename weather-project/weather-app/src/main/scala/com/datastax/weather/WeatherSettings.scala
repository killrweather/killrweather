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
package com.datastax.weather

import java.net.InetAddress

import scala.util.Try
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.cql.{AuthConf, NoAuthConf, PasswordAuthConf}
import com.typesafe.config.{Config, ConfigFactory}

import com.datastax.killrweather._


/**
 * Application settings. First attempts to acquire from the deploy environment.
 * If not exists, then from -D java system properties, else a default config.
 *
 * Settings in the environment such as: SPARK_HA_MASTER=local[10] is picked up first.
 *
 * Settings from the command line in -D will override settings in the deploy environment.
 * For example: sbt -Dspark.master="local[12]" run
 *
 * If you have not yet used Typesafe Config before, you can pass in overrides like so:
 *
 * {{{
 *   new Settings(ConfigFactory.parseString("""
 *      spark.master = "some.ip"
 *   """))
 * }}}
 *
 * Any of these can also be overriden by your own application.conf.
 *
 * @param conf Optional config for test
 */
class WeatherSettings(conf: Option[Config] = None) extends Settings(conf) {

  protected val killrweather = rootConfig.getConfig("killrweather")

  val AppName = killrweather.getString("app-name")
  val CassandraKeyspace = killrweather.getString("cassandra.keyspace")
  val CassandraTableRaw = killrweather.getString("cassandra.table.raw")
  val CassandraTableDailyTemp = killrweather.getString("cassandra.table.daily.temperature")
  val CassandraTableDailyPrecip = killrweather.getString("cassandra.table.daily.precipitation")
  val CassandraTableCumulativePrecip = killrweather.getString("cassandra.table.cumulative.precipitation")
  val CassandraTableSky = killrweather.getString("cassandra.table.sky")
  val CassandraTableStations = killrweather.getString("cassandra.table.stations")
  val DataLoadPath = killrweather.getString("data.load.path")
  val DataFileExtension = killrweather.getString("data.file.extension")
}

// @see http://www.warski.org/blog/2010/12/di-in-scala-cake-pattern/
// Interface
trait WeatherSettingsComponentImpl extends SettingsComponent { // For expressing dependencies
  override def Settings(conf: Option[Config] = None) = new WeatherSettings(conf) // Way to obtain the dependency
}

