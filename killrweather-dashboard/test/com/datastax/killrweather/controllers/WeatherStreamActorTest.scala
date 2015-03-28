package com.datastax.killrweather.controllers

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.datastax.killrweather.Weather.WeatherStation
import com.datastax.killrweather.WeatherStationId
import com.datastax.killrweather.controllers.WeatherStreamActor.WeatherUpdate
import com.datastax.killrweather.service.{WeatherService, WeatherStationInfo}
import org.mockito.Mockito._
import org.scalatest.FunSuiteLike
import org.scalatest.mock.MockitoSugar
import play.api.libs.json.Json

import scala.concurrent.Future

class WeatherStreamActorTest extends TestKit(ActorSystem("WeatherStreamActorTest")) with FunSuiteLike with MockitoSugar {

  import Implicits._

  test("Send weather update") {
    val outActor = TestProbe()
    val weatherService = mock[WeatherService]
    val stationId: WeatherStationId = WeatherStationId("station")
    val underTest = TestActorRef(new WeatherStreamActor(weatherService, outActor.ref, stationId))
    val station = WeatherStation(stationId.id, "", "", "", 1.0, 2.0, 3.0)
    val info: WeatherStationInfo = WeatherStationInfo(station, Seq())
    val weatherUpdate = Future.successful(Some(info))
    when(weatherService.getWeatherStation(stationId)).thenReturn(weatherUpdate)

    underTest ! WeatherUpdate

    verify(weatherService).getWeatherStation(stationId)
    outActor.expectMsg(s"""{"event":"weatherUpdate","data":${Json.toJson(info).toString()}}""")
  }
}
