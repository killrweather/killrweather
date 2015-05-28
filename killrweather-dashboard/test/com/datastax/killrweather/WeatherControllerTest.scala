package com.datastax.killrweather

import akka.actor.ActorSystem
import akka.testkit.{TestKit, TestProbe}
import com.datastax.killrweather.Weather.WeatherStation
import com.datastax.killrweather.WeatherEvent.{GetWeatherStationWithPrecipitation, GetWeatherStations}
import com.datastax.killrweather.controllers.Implicits._
import com.datastax.killrweather.controllers.WeatherController
import com.datastax.killrweather.service.{DayPrecipitation, WeatherStationInfo}
import org.scalatest.{FunSuiteLike, Matchers}
import play.api.libs.json.Json
import play.api.test.FakeRequest
import play.api.test.Helpers._

import scala.language.postfixOps

class WeatherControllerTest extends TestKit(ActorSystem("WeatherStreamActorTest")) with FunSuiteLike with Matchers {

    test("Return a list of weather stations") {
      val testProbe = TestProbe()
      val stations: Seq[WeatherStation] = Seq(WeatherStation("id", "name", "countryCode", "callSign", 1.0, 2.0, 3.0))

      val underTest = new WeatherController(testProbe.ref)

      val response = underTest.stations(FakeRequest())
      testProbe.expectMsg(GetWeatherStations)
      testProbe.reply(stations)
      status(response) should be(200)
      contentAsJson(response) should equal(Json.toJson(stations))
    }

    test("Show a dummy index page") {
      val testProbe = TestProbe()
      val underTest = new WeatherController(testProbe.ref)

      val response = underTest.index(FakeRequest())
      status(response) should be(303) // re-directed
    }

    test("Retrieve a single weather station") {
      val stationId = "12345"
      val station: WeatherStationInfo = WeatherStationInfo(WeatherStation(stationId, "Name", "GB", "BBB", 1.0, 2.0, 3.0),
        Seq(DayPrecipitation("2015-3-30", 10.0)))
      val testProbe = TestProbe()
      val underTest = new WeatherController(testProbe.ref)

      val result = underTest.station(stationId)(FakeRequest())

      testProbe.expectMsg(GetWeatherStationWithPrecipitation(stationId))
      testProbe.reply(Some(station))
      status(result) should be(200)
      contentAsString(result) should be(s"""{"weatherStation":{"id":"$stationId","name":"Name","countryCode":"GB","callSign":"BBB","lat":1.0,"long":2.0,"elevation":3.0},"dailyPrecipitation":[{"date":"2015-3-30","precipitation":10.0}]}""".stripMargin)
    }

    test("Retrieve a single weather station - not found") {
      val testProbe = TestProbe()
      val underTest = new WeatherController(testProbe.ref)

      val result = underTest.station("12345")(FakeRequest())

      testProbe.expectMsg(GetWeatherStationWithPrecipitation("12345"))
      testProbe.reply(None)
      status(result) should be(404)
    }
}
