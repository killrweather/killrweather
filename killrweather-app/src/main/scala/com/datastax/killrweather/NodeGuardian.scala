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
import akka.pattern.ask
import akka.pattern.gracefulStop
import akka.util.Timeout
import org.apache.spark.streaming.kafka.KafkaInputDStream
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.embedded.{Assertions, EmbeddedKafka}
import com.datastax.killrweather.compute._

/**
 * The `NodeGuardian` is the root of the primary KillrWeather deployed application.
 * It manages the worker actors and is Akka Cluster aware by extending [[ClusterAwareActor]].
 *
 * 1. Creates the [[RawDataPublisher]] which transforms raw weather data .gz files
 *    to line data and publishes to the Kafka topic created in [[KillrWeather]].
 *
 * 2. Creates the [[KafkaStreamActor]] which creates a streaming pipeline from Kafka to Cassandra,
 *    via Spark, which streams the raw data from Kafka, transforms each line of data to
 *    a [[com.datastax.killrweather.Weather.RawWeatherData]] (hourly per weather station),
 *    and saves the new data to the cassandra raw data table as it arrives.
 *
 * 3. Creates the compute actor supervisors [[TemperatureSupervisor]], [[PrecipitationSupervisor]],
 *    which in turn create the daily background compute workers that read the raw data,
 *    transform it, and persist to the daily temp and precip rollup tables respectively.
 *
 * 4. The first things we do after worker creation is to get weather station Ids
 *    from [[WeatherStationActor]] to hand to the daily background computation workers.
 *    We do this once, thus not in Actor.preStart()

 * NOTE: if `NodeGuardian` is ever put on an Akka router, multiple instances of the stream will
 * exist on the node. Might want to call 'union' on the streams in that case.
 */
class NodeGuardian(ssc: StreamingContext, kafkaServer: EmbeddedKafka,
                   settings: WeatherSettings)
  extends ClusterAwareActor with Assertions with ActorLogging {
  import WeatherEvent._
  import settings._

  implicit val timeout = Timeout(5.seconds)

  /* Creates the Kafka actors: */
  val kafka = context.actorOf(Props(new KafkaSupervisor(kafkaServer, ssc, settings, self)), "kafka")
  /* Ingests raw data via Spark and publishes to Kafka topic. */
  kafka ! PublishFeed

  /* The Spark/Cassandra computation actors: For the tutorial we just use 2005 for now. */
  val temperature = context.actorOf(Props(new TemperatureSupervisor(2005, ssc, settings)), "temperature")
  val precipitation = context.actorOf(Props(new PrecipitationActor(ssc, settings)), "precipitation")
  val station = context.actorOf(Props(new WeatherStationActor(ssc, settings, temperature, precipitation)), "weather-station")

  override def preStart(): Unit =
    log.info("Starting up.")

  override def postStop(): Unit =
    log.info("Shutting down.")

  /** On startup, actor is in an [[uninitialized]] state. */
  override def receive = uninitialized orElse super.receive

  /** When [[OutputStreamInitialized]] is received from the [[KafkaStreamActor]] after
    *   it creates and defines the [[KafkaInputDStream]], at which point the
      * - streaming checkpoint can be set
      * - the [[StreamingContext]] can be started
    * At this point, the actor changes state from [[uninitialized]] to [[initialized]]
    *   with [[ActorContext.become()]].
    */
  def uninitialized: Actor.Receive = {
    case OutputStreamInitialized => initialize()
  }

  /** When [[WeatherStationIds]] is received from [[WeatherStationActor]],
    *   it sends them to the compute actors. */
  def initialized: Actor.Receive = {
    case e: GetMonthlyTemperature => temperature forward e
    case e: GetPrecipitation      => precipitation forward e
    case e: GetWeatherStation     => station forward e
    case e: GetSkyConditionLookup => ???
    case StreamWeatherStationIds  => station forward StreamWeatherStationIds
    case PoisonPill               => gracefulShutdown()
  }

  def initialize(): Unit = {
    ssc.checkpoint(SparkCheckpointDir)
    ssc.start() // can not add more dstreams after this is started

    /* Requests weather station Ids from [[WeatherStationActor]]
    for running the daily background computations. */
    station ! PublishWeatherStationIds

    context become initialized

    log.info(s"Node is transitioning from 'uninitialized' to 'initialized'")
    context.system.eventStream.publish(NodeInitialized(self))
  }

  def gracefulShutdown(): Unit = {
    context.children foreach (c => awaitCond(gracefulStop(c, timeout.duration).isCompleted))
    log.info(s"Graceful stop completed.")
  }

}
