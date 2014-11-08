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

import scala.concurrent.duration._
import akka.cluster.Cluster
import akka.actor._
import akka.testkit.{DefaultTimeout, ImplicitSender, TestKit}
import com.datastax.killrweather.Weather.RawWeatherData
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.storage.StorageLevel
import org.scalatest._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming.StreamingEvent._
import com.datastax.spark.connector.streaming.TypedStreamingActor

trait AbstractSpec extends Suite with WordSpecLike with Matchers with BeforeAndAfterAll

abstract class ActorSparkSpec extends AkkaSpec with AbstractSpec {
  import com.datastax.spark.connector.streaming._
  import com.datastax.spark.connector._
  import KafkaEvent._
  import settings._

  val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.datastax.killrweather.KillrKryoRegistrator")
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  val sc = new SparkContext(conf)

  val ssc =  new StreamingContext(sc, Seconds(SparkStreamingBatchInterval)) // 1s

  CassandraConnector(conf).withSessionDo { session =>
    // insure for test we are not going to look at existing data, but new from the kafka actor processes
    session.execute(s"DROP KEYSPACE IF EXISTS $CassandraKeyspace")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"""CREATE TABLE $CassandraKeyspace.$CassandraTableRaw (
      wsid text, year int, month int, day int, hour int,temperature double, dewpoint double, pressure double,
      wind_direction int, wind_speed double,sky_condition int, sky_condition_text text, one_hour_precip double, six_hour_precip double,
      PRIMARY KEY ((wsid), year, month, day, hour)
     ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC)""")

    session.execute(s"""CREATE TABLE $CassandraKeyspace.$CassandraTableDailyTemp (
       wsid text,year int,month int,day int,high double,low double,mean double,variance double,stdev double,
       PRIMARY KEY ((wsid), year, month, day)
    ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)""")

    session.execute(s"""CREATE TABLE $CassandraKeyspace.$CassandraTableDailyPrecip (
       wsid text,year int,month int,day int,precipitation counter,
       PRIMARY KEY ((wsid), year, month, day)
    ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)""")
  }

  val kafkaActor: Option[ActorRef] = None

  def start(): Unit = {
    // for tests not creating a streaming output prior to calling start on the ssc - for test purposes
    if (kafkaActor.isEmpty) {
      ssc.actorStream[String](Props[TestStreamingActor], "stream", StorageLevel.MEMORY_ONLY)
        .flatMap(_.split("\\s+")).map(x => (1,x)).saveToCassandra(CassandraKeyspace, "make_streaming_happy")
    }

    ssc.start()
    loadData()
  }

  /* Loads data from /data/load files (because this is for a runnable demo.
   * Because we run locally vs against a cluster as a demo app, we keep that file size data small.
   * Using rdd.toLocalIterator will consume as much memory as the largest partition in this RDD,
   * which in this use case is 360 or fewer (if current year before December 31) small Strings. */
  def loadData(): Unit = kafkaActor match {
    case Some(actor) =>
      // store via kafka stream
      for (partition <- ByYearPartitions) {
        val toActor = (line: String) =>
          actor ! KafkaMessageEnvelope[String,String](KafkaTopicRaw, KafkaGroupId, line)

        ssc.sparkContext.textFile(partition.uri)
          .flatMap(_.split("\\n"))
          .toLocalIterator
          .foreach(toActor)
      }

    case None =>
    // not a kafka actor test, store directly
      for (partition <- ByYearPartitions) {
        ssc.sparkContext.textFile(partition.uri)
          .flatMap(_.split("\\n"))
          .map(_.split(","))
          .map(RawWeatherData(_))
          .saveToCassandra(CassandraKeyspace, CassandraTableRaw)
      }
  }

  override def afterAll() {
    deleteOnExit()
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

   val wsid = "010010:99999"

   val settings = new WeatherSettings()

   protected val cluster = Cluster(system)

   system.actorOf(Props(new MetricsListener(cluster)))

   protected val log = akka.event.Logging(system, system.name)

   override def afterAll() {
     system.shutdown()
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
      log.debug("Heap Memory: {} MB", used.doubleValue / 1024 / 1024)
    case _ => // no heap info
  }

  def logCpu(nodeMetrics: NodeMetrics): Unit = nodeMetrics match {
    case Cpu(address, timestamp, Some(systemLoadAverage), cpuCombined, processors) =>
      log.debug("System Load Avg: {} ({} processors)", systemLoadAverage, processors)
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
