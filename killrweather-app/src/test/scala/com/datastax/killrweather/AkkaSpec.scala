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

import java.io.{File => JFile}
import akka.cluster.Cluster
import akka.actor._
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import org.scalatest._

trait AbstractSpec extends Suite with WordSpecLike with Matchers with BeforeAndAfterAll

abstract class AkkaSpec extends TestKit(ActorSystem()) with AbstractSpec with ImplicitSender with DefaultTimeout {

   val settings = new Settings()

   protected val cluster = Cluster(system)

   system.actorOf(Props(new MetricsListener(cluster)))

   protected val log = akka.event.Logging(system, system.name)

   override def afterAll() {
     system.terminate()
  }
}

trait TestFileHelper {

  def fileFeed(path: String, extension: String): Set[JFile] = {
    println(s"path=$path, ext=$extension")
    new JFile(path).list.collect {
      case name if name.endsWith(extension) =>
        new JFile(s"$path/$name".replace("./", ""))
    }.toSet
  }
}

// http://doc.akka.io/docs/akka/2.4.2/scala/cluster-metrics.html#Subscribe_to_Metrics_Events
class MetricsListener(cluster: Cluster) extends Actor with ActorLogging {
  import akka.actor.ActorLogging
  import akka.actor.Actor
  import akka.cluster.Cluster
  import akka.cluster.metrics.ClusterMetricsEvent
  import akka.cluster.metrics.ClusterMetricsChanged
  import akka.cluster.ClusterEvent.CurrentClusterState
  import akka.cluster.metrics.NodeMetrics
  import akka.cluster.metrics.StandardMetrics.HeapMemory
  import akka.cluster.metrics.StandardMetrics.Cpu
  import akka.cluster.metrics.ClusterMetricsExtension

  val selfAddress = cluster.selfAddress

  override def preStart(): Unit = cluster.subscribe(self, classOf[ClusterMetricsChanged])
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive : Actor.Receive = {
    case ClusterMetricsChanged(clusterMetrics) =>
      clusterMetrics.filter(_.address == selfAddress) foreach { nodeMetrics =>
       logHeap(nodeMetrics)
       logCpu(nodeMetrics)
      }
    case state: CurrentClusterState => // ignore
  }

  def logHeap(nodeMetrics: NodeMetrics): Unit = nodeMetrics match {
    case HeapMemory(address, timestamp, used, committed, max) =>
      log.debug("Heap Memory: {} MB", used.doubleValue / 1024 / 1024)
    case _ => // no heap info
  }

  def logCpu(nodeMetrics: NodeMetrics): Unit = nodeMetrics match {
    case Cpu(address, timestamp, Some(systemLoadAverage), cpuCombined, cpuStolen, processors) =>
      log.debug("System Load Avg: {} ({} processors)", systemLoadAverage, processors)
    case _ => // no cpu info
  }
}
