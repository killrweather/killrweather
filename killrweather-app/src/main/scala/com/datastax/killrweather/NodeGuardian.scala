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

import com.datastax.killrweather.cluster.ClusterAwareNodeGuardian

import scala.concurrent.duration._
import akka.actor.{Actor, Props}
import org.apache.spark.streaming.kafka.KafkaInputDStream
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.embedded._
import org.apache.spark.SparkContext

/** A `NodeGuardian` manages the worker actors at the root of each KillrWeather
 * deployed application, where any special application logic is handled in the
 * implementer here, but the cluster work, node lifecycle and supervision events
 * are handled in [[ClusterAwareNodeGuardian]], in `killrweather/killrweather-core`.
 *
 */
class NodeGuardian(sc: SparkContext, kafka: EmbeddedKafka, settings: WeatherSettings)
  extends ClusterAwareNodeGuardian with AggregationActor {
  import WeatherEvent._
  import settings._

  /** The Spark/Cassandra computation actors: For the tutorial we just use 2005 for now. */
  val temperature = context.actorOf(Props(new TemperatureActor(sc, settings)), "temperature")
  val precipitation = context.actorOf(Props(new PrecipitationActor(sc, settings)), "precipitation")
  val station = context.actorOf(Props(new WeatherStationActor(sc, settings)), "weather-station")

  override def preStart(): Unit = {
    super.preStart()
    cluster.joinSeedNodes(Vector(cluster.selfAddress))
  }

  /** This node guardian's customer behavior once initialized. */
  override def initialized : Actor.Receive  = {
    case e: TemperatureRequest    => temperature forward e
    case e: PrecipitationRequest  => precipitation forward e
    case e: WeatherStationRequest => station forward e
    case GracefulShutdown => gracefulShutdown(sender())
  }

}
