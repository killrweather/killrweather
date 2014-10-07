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
package com.datastax.killrweather.api

import javax.servlet.http.HttpServletRequest

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.control.NonFatal
import akka.pattern.AskTimeoutException
import akka.util.Timeout
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json.NativeJsonSupport
import com.datastax.spark.connector.util.Logging

class WeatherServlet extends ScalatraServlet with FutureSupport with NativeJsonSupport with UrlGeneratorSupport with Logging {
  import com.datastax.killrweather.api.ApiData._

  protected implicit def timeout: Timeout = 5.seconds
  protected implicit def apiFormats: Formats = DefaultFormats
  protected implicit def executor: ExecutionContext = ExecutionContext.global
  override def jsonFormats: Formats = apiFormats

  protected def blueprintId(request: HttpServletRequest): Option[String] =
    for {
      validated <- UID(request)
      id <- validated.toOption
    } yield id.value

  def perPageParam(params: Params): Int = params.get("perPage").map(_.toInt) getOrElse 30

  // Only show necessary.
  notFound {
    status = 404
  }

  before() { contentType = "application/json" }

  error {
    case NonFatal(e) =>
      logError(s"$requestPath: ${e.getMessage}: $e")
      InternalServerError()
    case e: AskTimeoutException =>
      logError(s"""Ask timed out, returning status code 504. Request path = '$requestPath', requester: '${request.getRemoteHost}'. $e""")
      GatewayTimeout()
  }
}