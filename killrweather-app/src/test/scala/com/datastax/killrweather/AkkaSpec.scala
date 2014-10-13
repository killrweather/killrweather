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

import java.io.File

import akka.cluster.Cluster
import akka.actor.{Actor, ActorLogging, Props, ActorSystem}
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.datastax.killrweather.Weather.RawWeatherData
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.storage.StorageLevel
import org.joda.time.{DateTimeZone, DateTime}
import org.scalatest._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming.StreamingEvent._
import com.datastax.spark.connector.streaming.TypedStreamingActor

trait AbstractSpec extends Suite with WordSpecLike with Matchers with BeforeAndAfterAll

abstract class ActorSparkSpec extends AkkaSpec with AbstractSpec {
  import com.datastax.spark.connector.streaming._
  import com.datastax.spark.connector._
  import StreamingContext._
  import settings._

  val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  val sc = new SparkContext(conf)

  val ssc =  new StreamingContext(sc, Seconds(SparkStreamingBatchInterval)) // 1s

  /* Declare a required output stream. */

  /* Initialize data only if necessary. */
  // to run streaming, at least one output
  CassandraConnector(conf).withSessionDo { session =>
    session.execute(s"CREATE TABLE IF NOT EXISTS $CassandraKeyspace.words (word TEXT PRIMARY KEY, count COUNTER)")
    session.execute(s"TRUNCATE $CassandraKeyspace.words")
  }
  ssc.actorStream[String](Props[TestStreamingActor], "stream", StorageLevel.MEMORY_AND_DISK)
    .flatMap(_.split("\\s+"))
    .map(x => (x, 1))
    .reduceByKey(_ + _)
    .saveToCassandra(CassandraKeyspace, "words")

  val lines = ssc.sparkContext.textFile(s"$DataLoadPath/2005.csv.gz")
    .flatMap(_.split("\\n"))
    .map { case d => d.split(",")}
    .map(RawWeatherData(_))

  if (notInitialized)
    lines.saveToCassandra(CassandraKeyspace, CassandraTableRaw)
  else {
    ssc.textFileStream("./data/test-output").saveAsTextFiles(getClass.getSimpleName, "test.out")
    system.registerOnTermination(deleteOnExit())
  }

  ssc.start()

  def notInitialized: Boolean = {
    val test = new DateTime(DateTimeZone.UTC).withYear(2005).withMonthOfYear(1).withDayOfMonth(1)
    ssc.cassandraTable(CassandraKeyspace, CassandraTableRaw)
      .select("temperature").where("weather_station = ? AND year = ? AND month = ? AND day = ?",
        "010010:99999", test.getYear, test.getMonthOfYear, test.getDayOfMonth).count == 0
  }

  override def afterAll() {
    ssc.stop(true, false)
    super.afterAll()
  }

  private def deleteOnExit(): Unit = {
    import java.io.{ File => JFile }
    import scala.reflect.io.Directory

    val files = new JFile(".").list.collect {
      case path if path.startsWith(getClass.getSimpleName) && path.endsWith(".test.out") =>
        val dir = new Directory(new File(path))
        dir.deleteRecursively()
    }
  }
}

 //with SharedEmbeddedCassandra
abstract class AkkaSpec extends TestKit(ActorSystem()) with AbstractSpec with ImplicitSender with DefaultTimeout {

   val settings = new WeatherSettings()

   protected val cluster = Cluster(system)

   system.actorOf(Props(new MetricsListener(cluster)))

   protected val log = akka.event.Logging(system, system.name)

   override def afterAll() {
     import scala.concurrent.duration._
     system.shutdown()
     awaitCond(system.isTerminated, 3.seconds)
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


/** A very basic Akka actor which streams `String` event data to spark on receive. */
class TestStreamingActor extends TypedStreamingActor[String] {

  override def preStart(): Unit =
    context.system.eventStream.publish(ReceiverStarted(self))

  override def push(e: String): Unit = {
    super.push(e)
    // TODO
  }
}
