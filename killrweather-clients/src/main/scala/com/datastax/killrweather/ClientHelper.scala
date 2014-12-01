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

import scala.util.Try
import scala.util.control.NonFatal
import akka.util.ByteString
import com.typesafe.config.ConfigFactory

private[killrweather] trait ClientHelper {

  private val config = ConfigFactory.load
  protected val BasePort = 2550
  protected val DefaultPath = config.getString("killrweather.data.load.path")
  protected val DefaultExtension = config.getString("killrweather.data.file.extension")
  protected val DefaultTopic = config.getString("kafka.topic.raw")
  protected val DefaultGroup = config.getString("kafka.group.id")

  // val Pattern = """Content-Type.*?(/[^\s,]+)(?:,(/[^\s,]+))*""".r
  protected def parse(data: ByteString): Option[String] =
    Try(data.utf8String.split("Content-Type: application/x-www-form-urlencoded")(1).trim).toOption

  protected def fileFeed(path: String = DefaultPath, extension: String = DefaultExtension): Set[JFile] =
    new JFile(path).list.collect {
      case name if name.endsWith(extension) =>
        new JFile(s"$path/$name".replace("./", ""))
    }.toSet

  protected def getLines(file: JFile): Stream[String] = try {
    file match {
      case null =>
        throw new IllegalArgumentException("File must not be null.")
      case f if f.getAbsolutePath endsWith ".gz" =>
        scala.io.Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)))).getLines.toStream
      case f =>
        scala.io.Source.fromFile(file).getLines.toStream
    }
  } catch { case NonFatal(e) =>
      println(s"Error parsing lines from file $file: $e"); throw e
  }

}

