package com.datastax.killrweather.controllers

import com.datastax.killrweather.WeatherEvent.LoadSpec
import com.datastax.killrweather.service.LoadGenerationService
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.joda.time.{DateTime, Duration}
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import play.api.libs.json.Json
import play.api.test.Helpers._
import play.api.test._

class LoadGenerationControllerTest extends FunSuite with MockitoSugar with ScalaFutures with Matchers with BeforeAndAfterAll {

  test("Accepts load generation") {
    val loadGenerationService = mock[LoadGenerationService]
    val underTest = new LoadGenerationController(loadGenerationService)
    val request = FakeRequest("POST", "/").withJsonBody(Json.parse("""{"weatherStation":"chris","loadSpec":{"startDate":"2015-04-03T10:55:00.000Z", "endDate":"2015-04-15T09:10:00.000Z", "duration": 2000}}"""))

    val result = call(underTest.generateLoad(), request)

    status(result) should be(200)
    val time: DateTimeFormatter = ISODateTimeFormat.dateTime()
    verify(loadGenerationService).generateLoad("chris", LoadSpec(new DateTime(time.parseDateTime("2015-04-03T10:55:00.000Z")), time.parseDateTime("2015-04-15T09:10:00.000Z"), Duration.millis(2000)))
  }

  test("Invalid JSON returns bad request") {
    val loadGenerationService = mock[LoadGenerationService]
    val underTest = new LoadGenerationController(loadGenerationService)
    val request = FakeRequest("POST", "/").withJsonBody(Json.parse("""{"message":"this is not the json you are looking for"}"""))

    val result = call(underTest.generateLoad(), request)

    status(result) should be(400)
  }
}
