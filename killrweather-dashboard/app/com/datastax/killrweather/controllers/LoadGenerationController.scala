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
package com.datastax.killrweather.controllers

import org.joda.time.Duration
import org.joda.time.format.ISODateTimeFormat
import play.Logger
import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.mvc.{Action, BodyParsers, Controller}
import com.datastax.killrweather.WeatherEvent.{LoadSpec, LoadGenerationInput, WeatherStationId}
import com.datastax.killrweather.service.LoadGenerationService

import scala.language.postfixOps

class LoadGenerationController(loadGenerationService: LoadGenerationService) extends Controller {

  implicit val loadSpecReads: Reads[LoadSpec] = new Reads[LoadSpec] {

    val dateTimeFormatter = ISODateTimeFormat.dateTime()

    override def reads(json: JsValue): JsResult[LoadSpec] = {
      val startDate = (json \ "startDate").as[String]
      val endDate = (json \ "endDate").as[String]
      val duration = (json \ "duration").as[Long]
      JsSuccess(LoadSpec(dateTimeFormatter.parseDateTime(startDate), dateTimeFormatter.parseDateTime(endDate), Duration.millis(duration)))
    }
  }

  implicit val loadGenerationInput: Reads[LoadGenerationInput] = (
    (JsPath \ "weatherStation").read[WeatherStationId] and
    (JsPath \ "loadSpec").read[LoadSpec]
  )(LoadGenerationInput.apply _)

  def generateLoad(): Action[JsValue] = Action(BodyParsers.parse.json) { request =>
    Logger.info(s"Received load generation request ${request.body}")
    val input: JsResult[LoadGenerationInput] = request.body.validate[LoadGenerationInput]
    Logger.info(s"Parsed load spec $input")

    input.fold(
      errors => BadRequest(Json.obj("errorMessage" -> JsError.toFlatJson(errors))),
      stationAndSpec => {
        loadGenerationService.generateLoad(stationAndSpec.id, stationAndSpec.loadSpec)
        Ok("OK")
      }
    )
  }
}
