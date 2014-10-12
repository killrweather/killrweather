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

import akka.cluster.Cluster
import akka.actor.{Actor, ActorLogging, Props, ActorSystem}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import org.scalatest._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming.StreamingEvent._
import com.datastax.spark.connector.streaming.TypedStreamingActor

/** A very basic Akka actor which streams `String` event data to spark on receive. */
class TestStreamingActor extends TypedStreamingActor[String] {

  override def preStart(): Unit =
    context.system.eventStream.publish(ReceiverStarted(self))

  override def push(e: String): Unit = {
    super.push(e)
    // TODO
  }
}

abstract class ActorSparkSpec extends AkkaSpec with AbstractSpec {
  import org.apache.spark.streaming.StreamingContext.toPairDStreamFunctions
  import com.datastax.spark.connector.streaming._
  import settings.{CassandraKeyspace => keyspace}
  import settings._

  val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  val sc = new SparkContext(conf)

  val ssc =  new StreamingContext(sc, Seconds(120))

  // to run streaming, at least one output
  CassandraConnector(conf).withSessionDo { session =>
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.words (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute(s"TRUNCATE $keyspace.words")
  }
  ssc.actorStream[String](Props[TestStreamingActor], "stream", StorageLevel.MEMORY_AND_DISK)
    .flatMap(_.split("\\s+"))
    .map(x => (x, 1))
    .reduceByKey(_ + _)
    .saveToCassandra(keyspace, "words")

  ssc.start()

  override def afterAll() {
    ssc.stop(true, false)
    super.afterAll()
  }
}

 //with SharedEmbeddedCassandra
abstract class AkkaSpec extends TestKit(ActorSystem()) with AbstractSpec with ImplicitSender with DefaultTimeout {

   val settings = new WeatherSettings()

   protected val cluster = Cluster(system)

   system.actorOf(Props(new MetricsListener(cluster)))

   protected val log = akka.event.Logging(system, system.name)

   override def afterAll() {
    system.shutdown()
    awaitCond(system.isTerminated)
    super.afterAll()
  }
}


class MetricsListener(cluster: Cluster) extends Actor with ActorLogging {
  import akka.cluster.ClusterEvent.ClusterMetricsChanged
  import akka.cluster.ClusterEvent.CurrentClusterState
  import akka.cluster.NodeMetrics
  import akka.cluster.StandardMetrics.HeapMemory
  import akka.cluster.StandardMetrics.Cpu

  val selfAddress = cluster.selfAddress

  override def preStart(): Unit = cluster.subscribe(self, classOf[ClusterMetricsChanged])
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive = {
    case ClusterMetricsChanged(clusterMetrics) =>
      clusterMetrics.filter(_.address == selfAddress) foreach { nodeMetrics =>
       logHeap(nodeMetrics)
       logCpu(nodeMetrics)
      }
    case state: CurrentClusterState => // ignore
  }

  def logHeap(nodeMetrics: NodeMetrics): Unit = nodeMetrics match {
    case HeapMemory(address, timestamp, used, committed, max) =>
      log.info("Heap Memory: {} MB", used.doubleValue / 1024 / 1024)
    case _ => // no heap info
  }

  def logCpu(nodeMetrics: NodeMetrics): Unit = nodeMetrics match {
    case Cpu(address, timestamp, Some(systemLoadAverage), cpuCombined, processors) =>
      log.info("System Load Avg: {} ({} processors)", systemLoadAverage, processors)
    case _ => // no cpu info
  }
}
trait AbstractSpec extends Suite with WordSpecLike with Matchers with BeforeAndAfterAll