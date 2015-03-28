package com.datastax.killrweather

import com.datastax.killrweather.Weather.{DailyPrecipitation, WeatherStation}
import com.datastax.killrweather.controllers.WeatherController
import com.datastax.killrweather.infrastructure.WeatherStationDao
import com.datastax.killrweather.service.{DayPrecipitation, WeatherStationInfo, WeatherService}
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import play.api.test.FakeRequest
import play.api.test.Helpers._

import scala.concurrent.Future
import scala.language.postfixOps

class WeatherControllerTest  extends FunSuite with MockitoSugar with ScalaFutures with Matchers with BeforeAndAfterAll {

    test("Return a list of weather stations") {
      val weatherStationDao = mock[WeatherStationDao]
      when(weatherStationDao.retrieveWeatherStations()).thenReturn(Future.successful(Seq()))
      val weatherService = mock[WeatherService]
      val underTest = new WeatherController(weatherStationDao, weatherService)

      val response = underTest.stations(FakeRequest())
      status(response) should be(200)
    }

    test("Show a dummy index page") {
      val weatherStationDao = mock[WeatherStationDao]
      val weatherService = mock[WeatherService]
      val underTest = new WeatherController(weatherStationDao, weatherService)

      val response = underTest.index(FakeRequest())
      status(response) should be(303) // re-directed
    }

    test("Retrieve a single weather station") {
      val stationId = "12345"
      val station: WeatherStationInfo = WeatherStationInfo(WeatherStation(stationId, "Name", "GB", "BBB", 1.0, 2.0, 3.0),
        Seq(DayPrecipitation("2015-3-30", 10.0)))
      val weatherStationDao = mock[WeatherStationDao]
      val weatherService = mock[WeatherService]
      when(weatherService.getWeatherStation(WeatherStationId(stationId))).thenReturn(Future.successful(Some(station)))

      val underTest = new WeatherController(weatherStationDao, weatherService)

      val result = underTest.station(stationId)(FakeRequest())
      status(result) should be(200)
      contentAsString(result) should be(s"""{"weatherStation":{"id":"$stationId","name":"Name","countryCode":"GB","callSign":"BBB","lat":1.0,"long":2.0,"elevation":3.0},"dailyPrecipitation":[{"date":"2015-3-30","precipitation":10.0}]}""".stripMargin)
    }

    test("Retrieve a single weather station - not found") {
      val weatherStationDao = mock[WeatherStationDao]
      val weatherService = mock[WeatherService]
      when(weatherService.getWeatherStation(WeatherStationId("1234"))).thenReturn(Future.successful(None))
      val underTest = new WeatherController(weatherStationDao, weatherService)

      val result = underTest.station("1234")(FakeRequest())

      status(result) should be(404)
    }
}
