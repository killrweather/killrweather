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
import java.io.File

import akka.cluster.Cluster
import akka.http.model.HttpResponse

import scala.collection.immutable
import scala.util.Try
import akka.actor._
import akka.io.{IO, Tcp}
import akka.util.ByteString

object DataFeedApp extends App with ClientHelper {

  case object Ack extends Tcp.Event

  val system = ActorSystem("KillrWeather")

  val cluster = Cluster(system)
  cluster.joinSeedNodes(immutable.Seq(cluster.selfAddress))

  /** The 2 data feed actors publish messages to this actor which get published to Kafka for buffered streaming. */
  val guardian = system.actorSelection(cluster.selfAddress.copy(port = Some(BasePort)) + "/user/node-guardian")

  system.actorOf(Props(new AutomaticDataFeedActor(guardian, DefaultTopic, DefaultGroup)), "auto-data-feed")

  /** Accepts comma-separated list of files
    * curl -v -X POST -d "/path/to/file.csv" http://127.0.0.1:9051
    * or:
    * curl -v -X POST -d "/path/to/file.csv" http://127.0.0.1:9051
    * curl -v -X POST -d "/path/to/file.gz" http://127.0.0.1:9051
    */
  system.actorOf(Props(new DynamicDataFeedActor(guardian, DefaultTopic, DefaultGroup)), "dynamic-data-feed")

}

/** Because we run locally vs against a cluster as a demo app, we keep that file size data small.
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
class AutomaticDataFeedActor(guardian: ActorSelection, topic: String, group: String) extends Actor with ActorLogging with ClientHelper {
  import scala.concurrent.duration._
  import WeatherEvent._
  import context.dispatcher

  log.info("Starting automatic data file ingestion on {}", Cluster(context.system).selfAddress)

  val historicData = fileFeed()
  log.info(s"Ingesting {} data files", historicData.size)

  for(file <- historicData) {
    context.system.scheduler.scheduleOnce(5.seconds) {
      log.info(s"Ingesting {}", file.getAbsolutePath)
      val data = getLines(file)
      getLines(file) foreach { data =>
        context.system.scheduler.scheduleOnce(1.second) {
          guardian ! KafkaMessageEnvelope[String,String](topic, group, data)
        }
      }
    }
  }

  def receive : Actor.Receive = {
    case e => log.info("{}", e)
  }

}

class DynamicDataFeedActor(guardian: ActorSelection, topic: String, group: String) extends Actor with ActorLogging with ClientHelper {

  import Tcp._
  import context.system

  val remoteAddress = new InetSocketAddress("localhost", 9051)

  IO(Tcp) ! Bind(self, remoteAddress)

  def receive : Actor.Receive = {
    case b@Bound(local) => log.info("{}", b)
    case CommandFailed(_: Bind) => context stop self
    case Connected(remote, local) => connected(remote, local, sender)
  }

  def connected(remote: InetSocketAddress, local: InetSocketAddress, connection: ActorRef): Unit = {
    log.info("Connected remote[{}], local[{}]", remote, local)
    val handler = context.actorOf(Props(new DataFeedReceiverHandler(guardian, connection, topic, group)))
    connection ! Register(handler)
  }
}

private[killrweather] class DataFeedReceiverHandler(guardian: ActorSelection, connection: ActorRef, topic: String, group: String)
  extends Actor with ActorLogging with ClientHelper {

  import Tcp._
  import WeatherEvent._

  def receive: Actor.Receive = {
    case Received(data) => send(data, sender)
    case PeerClosed => context stop self
  }

  def send(data: ByteString, origin: ActorRef): Unit = {
    // tech debt
    Try(data.utf8String.split("Content-Type: application/x-www-form-urlencoded")(1).trim) map {
      value =>
        log.info("Received {} on {} from {}.", value, self.path.address, origin)
        connection ! HttpResponse()

        // TODO type
        val lines: Seq[String] = if (value contains File.separator) {
          value.split(",").map(new File(_)).filter(_.exists).flatMap {
            file => getLines(file)
          }
        } else Nil

        log.info(s"found ${lines.size} lines")
        lines foreach { line =>
          log.debug(s"sending to kafka $line")
          guardian ! KafkaMessageEnvelope[String,String](topic, group, line)
       }
    }
  }


}
