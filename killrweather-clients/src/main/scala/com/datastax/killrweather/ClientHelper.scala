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

import java.io.{BufferedInputStream, FileInputStream, File => JFile}
import java.util.zip.GZIPInputStream

import com.datastax.killrweather.Weather.Day

import scala.util.Try
import akka.http.model.{ContentTypes, RequestEntity, HttpHeader}
import akka.japi.Util.immutableSeq
import com.typesafe.config.ConfigFactory

private[killrweather] trait ClientHelper {
  import Sources._

  private val config = ConfigFactory.load
  protected val BasePort = 2550
  protected val HttpHost = config.getString("killrweather.http.host")
  protected val HttpPort = config.getInt("killrweather.http.port")
  protected val DefaultPath = config.getString("killrweather.data.load.path")
  protected val DefaultExtension = config.getString("killrweather.data.file.extension")
  protected val KafkaHosts = immutableSeq(config.getStringList("kafka.hosts")).toSet
  protected val KafkaTopic = config.getString("kafka.topic.raw")
  protected val KafkaKey = config.getString("kafka.group.id")
  protected val KafkaBatchSendSize = config.getInt("kafka.batch.send.size")
  protected val initialData: Set[FileSource] = new JFile(DefaultPath).list.collect {
      case name if name.endsWith(DefaultExtension) =>
        FileSource(new JFile(s"$DefaultPath/$name".replace("./", "")))
    }.toSet
}

private[killrweather] object Sources {
  sealed trait HttpSource extends Serializable {
    def header: HttpHeader
    def entity: RequestEntity
  }
  object HttpSource {
    def unapply[T](headers: Seq[HttpHeader], entity: RequestEntity): Option[HttpSource] =
      headers.collectFirst {
        case header if fileSource(header) => HeaderSource(header, entity)
        case header if entitySource(header, entity) => EntitySource(header, entity)
      }
  }
  case class EntitySource[T](header: HttpHeader, entity: RequestEntity) extends HttpSource {
    def extract: Iterator[T] = Iterator.empty // not supported yet
  }
  case class HeaderSource(header: HttpHeader, entity: RequestEntity, sources: Array[String]) extends HttpSource {
    def extract: Iterator[FileSource] = sources.map(new JFile(_)).filter(_.exists).map(FileSource(_)).toIterator
  }
  object HeaderSource {
    def apply(header: HttpHeader, entity: RequestEntity): HeaderSource =
      HeaderSource(header, entity, header.value.split(","))
  }
  case class FileSource(data: Array[String], name: String) {
    def days: Seq[Day] = data.map(Day(_)).toSeq
  }
  object FileSource {
    def apply(file: JFile): FileSource = {
      val src = file match {
        case f if f.getAbsolutePath endsWith ".gz" =>
          scala.io.Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))), "utf-8")
        case f =>
          scala.io.Source.fromFile(file, "utf-8")
      }
      val read = src.getLines.toList
      Try(src.close())
      FileSource(read.toArray, file.getName)
    }
  }

  private def fileSource(h: HttpHeader): Boolean =
    h.name == "X-DATA-FEED" && h.value.nonEmpty && h.value.contains(JFile.separator) // more validation..

  private def entitySource(h: HttpHeader, e: RequestEntity): Boolean =
    h.name == "X-DATA-FEED" && e.contentType == ContentTypes.`application/json` // more validation..
}

