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
import java.io.{File => JFile}

import scala.util.Try
import scala.concurrent.duration._
import scala.collection.immutable
import akka.actor._
import akka.cluster.Cluster
import akka.http.model.HttpResponse
import akka.io.{IO, Tcp}
import akka.util.ByteString

/** Run with
  * sbt -Dauto.import=false clients/run to run manual data file imports interactively via curl.
  * sbt clients/run for automatic data file import to Kafka.
  * 'auto.import' defaults to true.
  *
  * To do manual curl import: use PUT or POST
  * {{{
  *   curl -v -X PUT -d "/path/to/filename.csv" http://127.0.0.1:9051
  *   curl -v -X POST -d "./data/load/sf-2008.csv.gz" http://127.0.0.1:9051
  * }}}
  */
object DataFeedApp extends App with ClientHelper {
  import FileFeedEvent._

  val autoImport = Try(sys.props("auto.import").toBoolean) getOrElse true

  val system = ActorSystem("KillrWeather")

  val cluster = Cluster(system)
  cluster.joinSeedNodes(immutable.Seq(cluster.selfAddress))

  if (autoImport) {
    system.actorOf(Props(new AutomaticDataFeedActor(cluster)), "auto-data-feed") ! FileStreamEnvelope(for (f <- fileFeed()) yield FileStream(f))
  }

  /** Accepts comma-separated list of files
    * curl -v -X POST -d "/path/to/file.csv" http://127.0.0.1:9051
    * or:
    * curl -v -X POST -d "/path/to/file.gz,/path/to/file.csv" http://127.0.0.1:9051
    */
  system.actorOf(Props(new DynamicDataFeedActor(cluster)), "dynamic-data-feed")

}

/** Simulates real time weather events individually sent to the KillrWeather App for demos.
  *
  * Because we run locally vs against a cluster as a demo app, we keep that file size data small.
  * Using rdd.toLocalIterator will consume as much memory as the largest partition in this RDD,
  * which in this use case is 360 or fewer (if current year before December 31) small Strings.
  *
  * The ingested data is sent to the kafka actor for processing in the stream.
  *
  * RDD.toLocalIterator will consume as much memory as the largest partition in this RDD.
  * RDD.toLocalIterator uses allowLocal = false flag. `allowLocal` specifies whether the
  * scheduler can run the computation on the driver rather than shipping it out to the cluster
  * for short actions like first().
  */
class AutomaticDataFeedActor(cluster: Cluster) extends Actor with ActorLogging with ClientHelper {

  import WeatherEvent._
  import FileFeedEvent._
  import context.dispatcher

  log.info("Starting automatic data file ingestion on {}.", Cluster(context.system).selfAddress)

  def receive: Actor.Receive = {
    case FileStreamEnvelope(toStream) => start(toStream)
    case TaskCompleted                => stop()
  }

  def start(toStream: Set[FileStream]): Unit =
    for (fs <- toStream) {
      context.system.scheduler.scheduleOnce(2.seconds) {
        context.actorOf(Props(new FileFeedActor(cluster))) ! fs
      }
    }

  def stop(): Unit = if (context.children.isEmpty) context stop self
}

class DynamicDataFeedActor(cluster: Cluster) extends Actor with ActorLogging with ClientHelper {

  import Tcp._
  import context.system

  val remoteAddress = new InetSocketAddress("localhost", 9051)

  IO(Tcp) ! Bind(self, remoteAddress)

  def receive: Actor.Receive = {
    case b@Bound(local) => log.info("{}", b)
    case CommandFailed(_: Bind) => context stop self
    case Connected(remote, local) => connected(remote, local, sender)
  }

  def connected(remote: InetSocketAddress, local: InetSocketAddress, connection: ActorRef): Unit = {
    log.info("Connected remote[{}], local[{}]", remote, local)
    val handler = context.actorOf(Props(new DataFeedReceiverHandler(cluster, connection)))
    connection ! Register(handler)
  }
}

private[killrweather] class DataFeedReceiverHandler(cluster: Cluster, connection: ActorRef)
  extends Actor with ActorLogging with ClientHelper {

  import Tcp._
  import WeatherEvent._
  import FileFeedEvent._

  def receive: Actor.Receive = {
    case Received(data)           => send(data, sender)
    case PeerClosed               => context stop self
    case TaskCompleted
      if context.children.isEmpty => context stop self
  }

  // tech debt
  def send(data: ByteString, origin: ActorRef): Unit = parse(data) map {
    value =>
      log.info("Received {} on {} from {}.", value, self.path.address, origin)
      connection ! HttpResponse()

      if (value contains JFile.separator) {
        value.split(",").map(new JFile(_)).filter(_.exists) foreach {
          file =>
            log.info(s"Received {}", file)
            context.actorOf(Props(new FileFeedActor(cluster))) ! FileStream(file)
        }
      } else log.error("Received unsupported data type: {}", value)
  }
}

class FileFeedActor(cluster: Cluster) extends Actor with ActorLogging with ClientHelper {

  import WeatherEvent._
  import FileFeedEvent._
  import context.dispatcher

  /** The 2 data feed actors publish messages to this actor which get published to Kafka for buffered streaming. */
  val guardian = context.actorSelection(cluster.selfAddress.copy(port = Some(BasePort)) + "/user/node-guardian")

  def receive: Actor.Receive = {
    case e: FileStream => handle(e, sender)
  }

  def handle(e : FileStream, origin : ActorRef): Unit = {
    log.info(s"Ingesting {}", e.file.getAbsolutePath)
    e.getLines foreach { data =>
      context.system.scheduler.scheduleOnce(1.second) {
        log.debug(s"Sending '{}'", data)
        guardian ! KafkaMessageEnvelope[String, String](DefaultTopic, DefaultGroup, data)
      }
    }
    origin ! TaskCompleted
    context stop self
  }
}

