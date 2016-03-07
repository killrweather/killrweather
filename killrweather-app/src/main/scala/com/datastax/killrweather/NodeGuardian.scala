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
import akka.actor.{Actor, Props, ActorRef}
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.embedded._
import scala.Vector

/** A `NodeGuardian` manages the worker actors at the root of each KillrWeather
 * deployed application, where any special application logic is handled in the
 * implementer here, but the cluster work, node lifecycle and supervision events
 * are handled in [[ClusterAwareNodeGuardian]], in `killrweather/killrweather-core`.
 *
 * This `NodeGuardian` creates the [[KafkaStreamingActor]] which creates a streaming
 * pipeline from Kafka to Cassandra, via Spark, which streams the raw data from Kafka,
 * transforms data to [[com.datastax.killrweather.Weather.RawWeatherData]] (hourly per
 * weather station), and saves the new data to the cassandra raw data table on arrival.
 * 
 * All Domain Specific code should be define by a dedicated subclass of NodeGuardian
 * (like [[com.datastax.killrweather.WeatherNodeGuardian]]), 
 * which class should be referenced by an extension of NodeGuardianComponent
 * (like [[com.datastax.killrweather.WeatherNodeGuardianComponentImpl]]).
 */
abstract class NodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka)
  extends ClusterAwareNodeGuardian with AggregationActor with KafkaStreamingActorComponent with SettingsComponent {

  /** Creates the Kafka stream saving raw data and aggregated data to cassandra. */
  lazy val kafkaStreamingActor: KafkaStreamingActor = kafkaStreamingActor(kafka.kafkaParams, ssc, self)
  context.actorOf(Props(kafkaStreamingActor), "kafka-stream")

  override def preStart(): Unit = {
    super.preStart()
    cluster.joinSeedNodes(Vector(cluster.selfAddress))
  }

  /** When [[OutputStreamInitialized]] is received in the parent actor, [[ClusterAwareNodeGuardian]],
    * from the [[KafkaStreamingActor]] after it creates and defines the [[KafkaInputDStream]],
    * the Spark Streaming checkpoint can be set, the [[StreamingContext]] can be started, and the
    * node guardian actor moves from [[uninitialized]] to [[initialized]]with [[akka.actor.ActorContext.become()]].
    *
    * @see [[ClusterAwareNodeGuardian]]
    */
  override def initialize(): Unit = {
    super.initialize()
    ssc.checkpoint(Settings().SparkCheckpointDir)
    ssc.start() // currently can not add more dstreams once started

    context become initialized
  }

  /** This node guardian's customer behavior once initialized. */
  import BusinessEvent._
  def initialized: Actor.Receive = {
    case GracefulShutdown => gracefulShutdown(sender())
  }
}

// @see http://www.warski.org/blog/2010/12/di-in-scala-cake-pattern/
// Interface
trait NodeGuardianComponent { // For expressing dependencies
  def nodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka): NodeGuardian // Way to obtain the dependency
}

