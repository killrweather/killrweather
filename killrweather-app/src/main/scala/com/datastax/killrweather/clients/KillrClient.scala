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
package com.datastax.killrweather.clients

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor.{ActorRef, Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.killrweather.KafkaConsumer
import com.datastax.killrweather._
import com.datastax.spark.connector.embedded.{Assertions, ZookeeperConnectionString => zookeeper}



class KillrClient(settings: WeatherSettings, ssc: StreamingContext, guardian: ActorRef)
  extends Actor with ActorLogging with Assertions {
  import WeatherEvent._
  import Weather._
  import settings._

  val cluster = Cluster(context.system)

  val atomic = new AtomicInteger(0)
  val consumer = new KafkaConsumer(zookeeper, KafkaTopicRaw, KafkaGroupId, 1, 10, atomic)

  override def preStart(): Unit = {
    log.info("Starting up.")
    context.system.eventStream.subscribe(self, classOf[MemberUp])
    context.system.eventStream.subscribe(self, classOf[NodeInitialized])
  }

  override def postStop(): Unit =
    context.system.eventStream.unsubscribe(self)

  def receive: Actor.Receive = {
    case MemberUp(member) if member.address == cluster.selfAddress => start()
    case NodeInitialized(actor) =>
    case a: AnnualPrecipitation =>
    case a: TopKPrecipitation =>

  }

  def start(): Unit = {
    val data = ssc.cassandraTable[RawWeatherData](CassandraKeyspace, CassandraTableRaw)
    // wait until we get 8000 at a minimum via kafka..
    awaitCond(data.collect.size >= 8000, 30.seconds)
    val sample = Day(data.first)


    guardian ! GetPrecipitation(sample.wsid, sample.year)

    guardian ! GetTopKPrecipitation(sample.wsid, sample.year, k = 10)

    guardian ! GetDailyTemperature(sample)
  }

  def initialized(node: ActorRef): Unit = {
    log.info(s"Kafka has received ${atomic.get} messages so far.")
  }
}