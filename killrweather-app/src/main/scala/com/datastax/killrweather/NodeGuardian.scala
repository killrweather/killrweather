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
import akka.pattern.gracefulStop
import akka.util.Timeout
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.embedded.{Assertions, EmbeddedKafka}
import com.datastax.killrweather.compute._

/**
 * The `NodeGuardian` is the root of the primary KillrWeather deployed application.
 * It manages the worker actors and is Akka Cluster aware by extending [[ClusterAwareActor]].
 *
 * NOTE: if `NodeGuardian` is ever put on an Akka router, multiple instances of the stream will
 * exist on the node. Might want to call 'union' on the streams in that case.
 */
class NodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka, settings: WeatherSettings)
  extends ClusterAwareActor with Assertions with ActorLogging {
  import WeatherEvent._
  import settings._

  implicit val timeout = Timeout(3.seconds)

  /* Streams raw data from the Kafka topic to Cassandra. */
  context.actorOf(Props(new KafkaStreamActor(kafka, ssc, settings)), "kafka-stream")

  /* Reads raw data and publishes to Kafka topic - this would normally stream in. */
  val publisher = context.actorOf(Props(new RawDataPublisher(kafka.kafkaConfig, ssc, settings)))

  /** Create the computation actors: */
  val temperature = context.actorOf(Props(new TemperatureSupervisor(2005, ssc, settings)), "temperature")
  val precipitation = context.actorOf(Props(new PrecipitationActor(ssc, settings)), "precipitation")
  val station = context.actorOf(Props(new WeatherStationActor(ssc, settings)), "weather-station")

  /* The first things we do. Retrieves the set of weather station Ids
  to hand to the daily background computation workers. */
  override def preStart(): Unit = {
    publisher ! PublishFeed
    station ! GetWeatherStationIds
  }

  override def postStop(): Unit = {
    log.info("Shutting down.")
    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

  /** On startup, actor is in an uninitialized state. */
  override def receive = uninitialized orElse super.receive

  /** Once it receives weather station ids and [[OutputStreamInitialized]],
    * the guardian actor becomes initialized. */
  def uninitialized: Actor.Receive = {
    case e: WeatherStationIds =>
      temperature ! e
      precipitation ! e

    case OutputStreamInitialized =>
      ssc.checkpoint(SparkCheckpointDir)
      ssc.start() // can not add more dstreams after this is started
      context become initialized
  }

  def initialized: Actor.Receive = {
    case e: GetTemperature        => temperature forward e
    case e: GetWeatherStation     => station forward e
    case e: GetSkyConditionLookup =>
    case PoisonPill               => gracefulShutdown()
  }

  def gracefulShutdown(): Unit = {
    context.children foreach (c => awaitCond(gracefulStop(c, timeout.duration).isCompleted))
    log.info(s"Graceful stop completed.")
  }

}
