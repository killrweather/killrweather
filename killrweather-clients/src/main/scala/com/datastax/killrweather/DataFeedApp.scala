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
import org.reactivestreams.Publisher
import akka.http.Http.IncomingConnection
import akka.stream.FlowMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.actor._
import akka.cluster.Cluster
import akka.http.model._
import akka.http.model.HttpMethods._
import akka.io.IO
import akka.util.Timeout

/** Run with
  * sbt -Dauto.import=false clients/run to run manual data file imports interactively via curl.
  * sbt clients/run for automatic data file import to Kafka.
  * 'auto.import' defaults to true.
  *
  * To do manual curl import:
  * {{{
  *   curl -v -X POST --header "X-DATA-FEED: ./data/load/sf-2008.csv.gz" http://127.0.0.1:8080/weather/data
  * }}}
  *
  * NOTE does not support running on Windows
  */
object DataFeedApp extends App with ClientHelper {
  import FileFeedEvent._

  val autoImport = Try(sys.props("auto.import").toBoolean) getOrElse true

  val system = ActorSystem("KillrWeather")

  val cluster = Cluster(system)
  cluster.joinSeedNodes(immutable.Seq(cluster.selfAddress))

  system.actorOf(Props(new DynamicDataFeedActor(cluster)), "dynamic-data-feed")

  if (autoImport) {
    val envelope = FileStreamEnvelope(fileFeed().toArray:_*)
    system.actorOf(Props(new AutomaticDataFeedActor(cluster)), "auto-data-feed") ! envelope
  }
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

  def receive: Actor.Receive = {
    case e @ FileStreamEnvelope(toStream) => start(e)
    case TaskCompleted                    => stop()
  }

  def start(envelope: FileStreamEnvelope): Unit = {
    log.info("Starting data file ingestion on {}.", Cluster(context.system).selfAddress)

    envelope.files.map {
      case f if f == envelope.files.head =>
        context.system.scheduler.scheduleOnce(2.second) {
          context.actorOf(Props(new FileFeedActor(cluster))) ! envelope
        }
      case f =>
        context.system.scheduler.scheduleOnce(20.seconds) {
          context.actorOf(Props(new FileFeedActor(cluster))) ! envelope
        }
    }

    for (fs <- envelope.files) context.system.scheduler.scheduleOnce(20.seconds)
    context.actorOf(Props(new FileFeedActor(cluster))) ! envelope
  }

  def stop(): Unit = if (context.children.isEmpty) context stop self
}

class DynamicDataFeedActor(cluster: Cluster) extends Actor with ActorLogging with ClientHelper {
  import akka.http.Http
  import FileFeedEvent._
  import context.dispatcher

  implicit val system = context.system
  implicit val materializer = FlowMaterializer()
  implicit val askTimeout: Timeout = 500.millis

  val ctx = "/weather/data"
  val xheader = "X-DATA-FEED"

  IO(Http) ! Http.Bind("localhost", 8080)

  val toFiles = (headers: Seq[HttpHeader]) => headers.collectFirst {
    case header if isValid(header) =>
      header.value.split(",").map(new JFile(_)).filter(_.exists)
  }

  val requestHandler: HttpRequest => HttpResponse = {
    case HttpRequest(POST, Uri.Path(ctx), headers, entity, _) =>
      toFiles(headers) map { files =>
        log.info(s"Received {}", files.mkString)
        context.actorOf(Props(new FileFeedActor(cluster))) ! FileStreamEnvelope(files:_*)
        HttpResponse(200, entity = HttpEntity(MediaTypes.`text/html`, s"POST [$entity] successful."))
      } getOrElse
        HttpResponse(404, entity = s"Unknown resource '$entity'")

    case _: HttpRequest => HttpResponse(400, entity = "Unsupported request")
  }

  def receive : Actor.Receive = {
    case Http.ServerBinding(local, stream) => bound(local, stream)
    case WeatherEvent.TaskCompleted => // ignore
  }

  def bound(localAddress: InetSocketAddress, connectionStream: Publisher[IncomingConnection]): Unit = {
    log.info("Connected to [{}] with [{}]", localAddress, connectionStream)
    Source(connectionStream).foreach({
      case Http.IncomingConnection(remoteAddress, requestProducer, responseConsumer) =>
        log.info("Accepted new connection from {}.", remoteAddress)
        Source(requestProducer).map(requestHandler).to(Sink(responseConsumer)).run()
    })
  }

  def isValid(header: HttpHeader): Boolean =
    header.name == xheader && header.value.nonEmpty && header.value.contains(JFile.separator)
}

class FileFeedActor(cluster: Cluster) extends Actor with ActorLogging with ClientHelper {

  import WeatherEvent._
  import FileFeedEvent._
  import context.dispatcher

  /** The 2 data feed actors publish messages to this actor which get published to Kafka for buffered streaming. */
  val guardian = context.actorSelection(cluster.selfAddress.copy(port = Some(BasePort)) + "/user/node-guardian")

  def receive : Actor.Receive = {
    case e: FileStream => handle(e, sender)
  }

  def handle(e : FileStream, origin : ActorRef): Unit = {
    log.info(s"Ingesting {}", e.file.getAbsolutePath)
    e.getLines foreach { data =>
      context.system.scheduler.scheduleOnce(2.second) {
        log.debug(s"Sending '{}'", data)
        guardian ! KafkaMessageEnvelope[String, String](DefaultTopic, DefaultGroup, data)
      }
    }
    origin ! TaskCompleted
    context stop self
  }
}

