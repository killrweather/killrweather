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
import com.datastax.killrweather.clients.KillrClient
import org.apache.spark.streaming.{Seconds, StreamingContext}
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
  kafka.createTopic(settings.KafkaTopicRaw)

  /** Configures Spark.
    * The appName parameter is a name for your application to show on the cluster UI.
    * master is a Spark, Mesos or YARN cluster URL, or a special “local[*]” string to run in local mode.
    *
    * When running on a cluster, you will not want to launch the application with spark-submit and receive it there.
    * For local testing and unit tests, you can pass 'local[*]' to run Spark Streaming in-process
    * (detects the number of cores in the local system).
    *
    * Note that this internally creates a SparkContext (starting point of all Spark functionality)
    * which can be accessed as ssc.sparkContext.
    */
  lazy val conf = new SparkConf().setAppName(getClass.getSimpleName)
    .setMaster(settings.SparkMaster)
    .set("spark.cassandra.connection.host", settings.CassandraHosts)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.datastax.killrweather.KillrKryoRegistrator")
    .set("spark.cleaner.ttl", settings.SparkCleanerTtl.toString)

  lazy val sc = new SparkContext(conf)

  /** Creates the Spark Streaming context. */
  lazy val ssc = new StreamingContext(sc, Seconds(settings.SparkStreamingBatchInterval))
 
  /** Creates the ActorSystem. */

  val system = ActorSystem(AppName, rootConfig)

  /* The root supervisor Actor of our app. */
  val guardian = system.actorOf(Props(new NodeGuardian(ssc, kafka, settings)), "node-guardian")

  val client = system.actorOf(Props(new KillrClient(settings, ssc, guardian)), "client")

  system.registerOnTermination {
    kafka.shutdown()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    guardian ! PoisonPill
  }

}
