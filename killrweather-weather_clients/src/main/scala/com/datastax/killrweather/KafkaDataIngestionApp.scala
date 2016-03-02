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
import akka.stream.{ActorMaterializerSettings, ActorMaterializer}
import akka.actor._
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
import com.datastax.killrweather._


/** Run with: sbt clients/run for automatic data file import to Kafka.
  *
  * To do manual curl import:
  * {{{
  *   curl -v -X POST --header "X-DATA-FEED: ./data/load/sf-2008.csv.gz" http://127.0.0.1:8080/weather/data
  * }}}
  *
  * NOTE does not support running on Windows
  */
object KafkaDataIngestionApp extends App {

  /** Creates the ActorSystem. */
  val system = ActorSystem("KillrWeather", ConfigFactory.parseString("akka.remote.netty.tcp.port = 2551"))

  /* The root supervisor and fault tolerance handler of the data ingestion nodes. */
  val guardian = system.actorOf(Props[HttpNodeGuardian], "node-guardian")

  system.registerOnTermination {
    guardian ! PoisonPill
  }
}
