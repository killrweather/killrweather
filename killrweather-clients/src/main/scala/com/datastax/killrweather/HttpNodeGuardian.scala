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

import java.net.InetSocketAddress

import scala.concurrent.Future
import scala.concurrent.duration._
import org.reactivestreams.Publisher
import org.apache.commons.io.{IOUtils, LineIterator} 
import akka.stream.{ActorMaterializerSettings, ActorMaterializer}
import akka.actor._
import akka.event.LoggingAdapter
import akka.cluster.Cluster
import akka.util.Timeout
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.routing.BalancingPool
import kafka.producer.ProducerConfig
import kafka.serializer.StringEncoder
import com.typesafe.config.ConfigFactory
import com.datastax.spark.connector.embedded._
import com.datastax.killrweather.cluster.ClusterAwareNodeGuardian
import com.datastax.spark.connector.embedded.KafkaEvent.KafkaMessageEnvelope

/**
 * Transforms raw weather data .gz files to line data and publishes to the Kafka topic.
 *
 * Simulates real time weather events individually sent to the KillrWeather App for demos.
 * @see [[HttpDataFeedActor]] for sending data files via curl instead.
 *
 * Because we run locally vs against a cluster as a demo app, we keep that file size data small.
 * Using rdd.toLocalIterator will consume as much memory as the largest partition in this RDD,
 * which in this use case is 360 or fewer (if current year before December 31) small Strings.
 *
 * The ingested data is sent to the kafka actor for processing in the stream.
 */
abstract class HttpNodeGuardian extends ClusterAwareNodeGuardian with KafkaInterface with ClientHelper with HttpNodeGuardianComponent{
  
  log.info("UriPath: {}.", uriPath)

  cluster.joinSeedNodes(Vector(cluster.selfAddress))


  /** The [[KafkaPublisherActor]] as a load-balancing pool router
    * which sends messages to idle or less busy routees to handle work. */
  val router = context.actorOf(BalancingPool(5).props(
    Props(new KafkaPublisherActor(KafkaHosts, KafkaBatchSendSize))), "kafka-ingestion-router")

  /** Wait for this node's [[akka.cluster.MemberStatus]] to be
    * [[akka.cluster.ClusterEvent.MemberUp]] before starting work, which means
    * it's membership in the [[Cluster]] node ring has been gossipped, and we
    * can leverage the cluster's adaptive load balancing which will route data
    * to the `KillrWeatherApp` nodes based on most healthy, by their health metrics
    * - cpu, system load average and heap. */
  cluster registerOnMemberUp {

    /* As http data is received, publishes to Kafka. */
    context.actorOf(BalancingPool(10).props(
      Props(new HttpDataFeedActor(cluster, router, uriPath))), "dynamic-data-feed")

      /* Handles initial data ingestion in Kafka for running as a demo. */
      for (fs <- initialData) {
        publishDataToKafka(log, fs, cluster, router, KafkaTopic, KafkaKey)
      }
  }

  def initialized: Actor.Receive = {
    case BusinessEvent.TaskCompleted => // ignore for now
  }
}

/** The KafkaPublisherActor receives initial data on startup (because this
  * is for a runnable demo) and also receives data in runtime.
  *
  * Publishes [[com.datastax.spark.connector.embedded.KafkaEvent.KafkaMessageEnvelope]]
  * to Kafka on a sender's behalf. Multiple instances are load-balanced in the [[HttpNodeGuardian]].
  */
class KafkaPublisherActor(val producerConfig: ProducerConfig) extends KafkaProducerActor[String,String] {

  def this(hosts: Set[String], batchSize: Int) = this(KafkaProducer.createConfig(
    hosts, batchSize, "async", classOf[StringEncoder].getName))

}

/** An Http server receiving requests containing header or entity based data which it sends to Kafka.
  * by delegating to the [[KafkaPublisherActor]]. */
class HttpDataFeedActor(cluster: Cluster, kafka: ActorRef, uriPath:String) extends Actor with KafkaInterface with ActorLogging with ClientHelper {

  import Sources._
  import context.dispatcher
  import akka.stream.scaladsl._
  import akka.stream.scaladsl.Flow
  import HttpProtocols._
  import MediaTypes._
  import HttpCharsets._

  implicit val system = context.system

  implicit val askTimeout: Timeout = 500.millis

  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system)
  )
  
  val requestHandler: HttpRequest => HttpResponse = {
    case HttpRequest(HttpMethods.POST, Uri.Path(uriPath), headers, entity, _) =>
      HttpSource.unapply(headers,entity).collect { case hs: HeaderSource =>
        hs.extract.foreach({ fs: FileSource =>
          publishDataToKafka(log, fs, cluster, kafka, KafkaTopic, KafkaKey)
        })
        HttpResponse(200, entity = HttpEntity(`text/html` withCharset `UTF-8`, s"POST [${hs.sources.mkString}] successful."))
      }.getOrElse(HttpResponse(404, entity = "Unsupported request") )
    case _: HttpRequest =>
      HttpResponse(400, entity = "Unsupported request")
  }

  Http(system)
    .bind(interface = HttpHost, port = HttpPort)
    .map { case connection  =>
      log.info("Accepted new connection from " + connection.remoteAddress)
      connection.handleWithSyncHandler(requestHandler)
    }

  def receive : Actor.Receive = {
    case e =>
  }
}

trait KafkaInterface {
  def publishDataToKafka(log: LoggingAdapter, fs: Sources.FileSource, cluster: Cluster, router: ActorRef, KafkaTopic: String, KafkaKey: String) = {
    log.info("Starting data ingestion on {}.", cluster.selfAddress)

    var l = 0l
    var gap = 10L

    log.info(s"Ingesting {} and publishing data points to Kafka topic '{}'.", fs.name, KafkaTopic)
    try {
      val lineIterator = fs.data
      while (lineIterator.hasNext()) {
        val line: String = lineIterator.next().toString;
        router ! KafkaMessageEnvelope[String, String](KafkaTopic, KafkaKey, line)
        l += 1;
        if ((l % gap) == 0) {
          log.info("{} data rows from {} have been sent to {}.", l, fs.name, cluster.selfAddress)
          gap *= 10L
        }
      }
    } finally {
      fs.data.close()
    }
    
    log.info("All {} data rows from {} have been sent to {}.", l, fs.name, cluster.selfAddress)
  }
}

// @see http://www.warski.org/blog/2010/12/di-in-scala-cake-pattern/
// Interface
trait HttpNodeGuardianComponent { // For expressing dependencies
  def uriPath:String // Way to obtain the dependency
}

