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

import akka.actor.{ActorSystem, PoisonPill, Props}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector.embedded.EmbeddedKafka

/** Runnable. Requires running these in cqlsh
  * {{{
  *   cqlsh> source 'create-timeseries.cql';
  *   cqlsh> source 'load-timeseries.cql';
  * }}}
  *
  * See: https://github.com/killrweather/killrweather/wiki/2.%20Code%20and%20Data%20Setup#data-setup
  */
object KillrWeatherApp extends App {

  val settings = new WeatherSettings
  import settings._

  /** Starts the Kafka broker and Zookeeper. */
  val kafka = new EmbeddedKafka

  /** Creates the raw data topic. */
  kafka.createTopic(KafkaTopicRaw)

  /** Configures Spark. */
  lazy val conf = new SparkConf().setAppName(getClass.getSimpleName)
    .setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.datastax.killrweather.KillrKryoRegistrator")
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  lazy val sc = new SparkContext(conf)

  /** Creates the Spark Streaming context. */
  lazy val ssc = new StreamingContext(sc, Milliseconds(500))
 
  /** Creates the ActorSystem. */
  val system = ActorSystem(AppName, rootConfig)

  /* The root supervisor and traffic controller of the app. All inbound messages go through this actor. */
  val guardian = system.actorOf(Props(new NodeGuardian(ssc, kafka, settings)), "node-guardian")

  system.registerOnTermination {
    kafka.shutdown()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    guardian ! PoisonPill
  }
}
