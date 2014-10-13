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

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, PoisonPill, Props}
import com.typesafe.config.Config
import com.datastax.spark.connector.embedded.EmbeddedKafka

/** Runnable: for running WeatherCenter from command line or IDE. */
object RunnableKillrWeather extends App with KillrWeather

/** Used to run [[RunnableKillrWeather]] and [[com.datastax.killrweather.api.WeatherServletContextListener]] */
trait KillrWeather extends WeatherApp {
  import WeatherEvent._

  override val settings = new WeatherSettings()

  /** Starts the Kafka broker and Zookeeper. */
  lazy val kafka = new EmbeddedKafka

  /** Creates the ActorSystem. */
  val system = ActorSystem(settings.AppName)

  system.registerOnTermination {
    kafka.shutdown()
    guardian ! PoisonPill
  }

  /** Creates the raw data topic. */
  kafka.createTopic(settings.KafkaTopicRaw)

  /* The root supervisor Actor of our app. */
  val guardian = system.actorOf(Props(new NodeGuardian(ssc, kafka, settings)), "node-guardian")
  guardian ! GetWeatherStationIds

  ssc.awaitTermination()
}

/**
 * Application settings. First attempts to acquire from the deploy environment.
 * If not exists, then from -D java system properties, else a default config.
 *
 * @param conf Optional config for test
 */
final class WeatherSettings(conf: Option[Config] = None) extends Settings(conf) {
  import scala.concurrent.duration.Duration

  val CassandraKeyspace = killrweather.getString("cassandra.keyspace")
  val CassandraTableRaw = killrweather.getString("cassandra.table.raw")
  val CassandraTableDailyTemp = killrweather.getString("cassandra.table.daily.temperature")
  val CassandraTableDailyPrecip = killrweather.getString("cassandra.table.daily.precipitation")
  val CassandraTableSky = killrweather.getString("cassandra.table.sky")
  val CassandraTableStations = killrweather.getString("cassandra.table.stations")

  //val KafkaHosts: immutable.Seq[String] = Util.immutableSeq(timeseries.getStringList("kafka.hosts"))
  val KafkaGroupId = killrweather.getString("kafka.group.id")
  val KafkaTopicRaw = killrweather.getString("kafka.topic.raw")
  val KafkaBatchSendSize = killrweather.getInt("kafka.batch.send.size")

  val SparkCheckpointDir = killrweather.getString("spark.checkpoint.dir")

  // can't upgrade config until spark does :( could not get around that in the build
  val DailyTemperatureTaskInterval = Duration(killrweather.getLong("data.daily.temperature-task-interval-min"), TimeUnit.MILLISECONDS)

  val DataLoadPath = killrweather.getString("data.load.path")

  val DataYearRange: Range = {
    val s = killrweather.getInt("data.raw.year.start")
    val e = killrweather.getInt("data.raw.year.end")
    s to e
  }
}
