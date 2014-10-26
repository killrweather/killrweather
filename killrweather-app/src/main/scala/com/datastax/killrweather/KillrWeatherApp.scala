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

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/** Runnable. */
object KillrWeatherApp extends App {
  import akka.actor.{ActorSystem, PoisonPill, Props}
  import com.datastax.spark.connector.embedded.EmbeddedKafka

  val settings = new WeatherSettings
  import settings._

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
  lazy val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  lazy val sc = new SparkContext(conf)

  /** Creates the Spark Streaming context. */
  lazy val ssc =  new StreamingContext(sc, Seconds(SparkStreamingBatchInterval))

  /** Starts the Kafka broker and Zookeeper. */
  lazy val kafka = new EmbeddedKafka

  /** Creates the ActorSystem. */
  val system = ActorSystem(settings.AppName)

  /** Creates the raw data topic.
    *
    * For time series data you want to randomize the messages on partitions.
    * - If you are using the null partitioner and the 0.8.1.1 (or below) producer, watch out for this,
    *   which is improved in the 0.8.2 producer: http://tinyurl.com/kln5rrv (from https://cwiki.apache.org)
    * - You could use timeUUID as the key (for the partition which you will have that back in your consumer
    *   if you can access the MessageAndMetaData
    *   https://github.com/apache/kafka/blob/trunk/core/src/main/scala/kafka/message/MessageAndMetadata.scala ),
    *   and then have that as part of the insert in a clustered column to cassandra for the other fields for
    *   your partition key.
    */
  kafka.createTopic(settings.KafkaTopicRaw)

  val brokers = Set(s"${kafka.kafkaConfig.hostName}:${kafka.kafkaConfig.port}") // TODO the right way

  /* The root supervisor Actor of our app. */
  val guardian = system.actorOf(Props(new NodeGuardian(ssc, kafka, brokers, settings)), "node-guardian")

  system.registerOnTermination {
    kafka.shutdown()
    ssc.stop(stopSparkContext = true, stopGracefully = true)
    guardian ! PoisonPill
  }

  ssc.awaitTermination()
}
