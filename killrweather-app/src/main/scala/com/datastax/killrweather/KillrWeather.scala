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

import java.util.concurrent.atomic.AtomicBoolean
import akka.actor._
import akka.cluster.Cluster
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.SparkConf
import com.datastax.spark.connector.embedded.EmbeddedKafka
import scala.concurrent.Future

abstract class KillrWeather(system: ExtendedActorSystem) extends Extension with NodeGuardianComponent {
  import BusinessEvent.GracefulShutdown

  import system.dispatcher

  system.registerOnTermination(shutdown())

  protected val log = akka.event.Logging(system, system.name)

  protected val _isRunning = new AtomicBoolean(false)

  protected val _isTerminated = new AtomicBoolean(false)

  val settings = new WeatherSettings
  import settings._

  implicit private val timeout = system.settings.CreationTimeout

  /** Starts the Kafka broker and Zookeeper. */
  private val kafka = new EmbeddedKafka

  /** Creates the raw data topic. */
  kafka.createTopic(KafkaTopicRaw)

  /** Configures Spark. */
  protected val conf = new SparkConf().setAppName(getClass.getSimpleName)
    .setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  /** Creates the Spark Streaming context. */
  protected val ssc = new StreamingContext(conf, Milliseconds(SparkStreamingBatchInterval))

  /* The root supervisor and traffic controller of the app. All inbound messages go through this actor. */
  // NodeGuardian is provided by the NodeGuardianComponent
  val nodeGuardianInstance: NodeGuardian = nodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka, settings: WeatherSettings)
  private val guardian = system.actorOf(Props(nodeGuardianInstance), "node-guardian")

  private val cluster = Cluster(system)

  val selfAddress: Address = cluster.selfAddress

  cluster.joinSeedNodes(Vector(selfAddress))

  def isRunning: Boolean = _isRunning.get

  def isTerminated: Boolean = _isTerminated.get

  private def shutdown(): Unit = if (!isTerminated) {
    import akka.pattern.ask
    if (_isTerminated.compareAndSet(false, true)) {
      log.info("Node {} shutting down", selfAddress)
      cluster leave selfAddress
      kafka.shutdown()
      ssc.stop(stopSparkContext = true, stopGracefully = true)

      (guardian ? GracefulShutdown).mapTo[Future[Boolean]]
        .onComplete { _ =>
        system.terminate()
        // http://stackoverflow.com/questions/12436397/how-to-wait-for-akka-actor-system-to-terminate
        import scala.concurrent.Await
        Await.result(system.whenTerminated, timeout.duration)
      }
    }
  }
}
