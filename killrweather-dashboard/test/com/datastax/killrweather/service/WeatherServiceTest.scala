package com.datastax.killrweather.service

import com.datastax.killrweather.Weather.{DailyPrecipitation, WeatherStation}
import com.datastax.killrweather.WeatherStationId
import com.datastax.killrweather.infrastructure.WeatherStationDao
import org.mockito.Mockito
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.Future

class WeatherServiceTest extends FunSuite with MockitoSugar with Matchers with ScalaFutures {
  val id: WeatherStationId = WeatherStationId("1234")

  test("Gets weather station and daily precipitation") {
    //given
    val mockWeatherStationDao = mock[WeatherStationDao]
    val underTest = new WeatherService(mockWeatherStationDao)
    val weatherStation = WeatherStation("1234", "Denver", "US", "BBB", 1.0, 2.0, 3.0)
    val dailyPrecip = DailyPrecipitation("1234", 2015, 4, 1, 10.0)
    Mockito.when(mockWeatherStationDao.retrieveWeatherStation(id)).thenReturn(Future.successful(Some(weatherStation)))
    Mockito.when(mockWeatherStationDao.retrieveDailyPrecipitation(id, 50)).thenReturn(Future.successful(Seq(dailyPrecip)))

    //when
    val weatherStationInfo: Future[Option[WeatherStationInfo]] = underTest.getWeatherStation(id)

    //then
    whenReady(weatherStationInfo) { result =>
      result.get.dailyPrecipitation should equal(Seq(DayPrecipitation("2015-4-1", 10.0)))
      result.get.weatherStation should equal(weatherStation)
    }
  }

  test("Get weather station and daily precipitation for station that doesn't exist") {
    //given
    val mockWeatherStationDao = mock[WeatherStationDao]
    val underTest = new WeatherService(mockWeatherStationDao)
    val dailyPrecip = DailyPrecipitation("1234", 2015, 4, 1, 10.0)
    Mockito.when(mockWeatherStationDao.retrieveWeatherStation(id)).thenReturn(Future.successful(None))
    Mockito.when(mockWeatherStationDao.retrieveDailyPrecipitation(id, 50)).thenReturn(Future.successful(Seq(dailyPrecip)))

    //when
    val weatherStationInfo: Future[Option[WeatherStationInfo]] = underTest.getWeatherStation(id)

    //then
    whenReady(weatherStationInfo) { result =>
      result should equal(None)
    }
  }
}
