package com.datastax.killrweather.controllers

import akka.actor.ActorSystem
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.datastax.killrweather.DashboardApiActor.GetWeatherStationWithPrecipitation
import com.datastax.killrweather.Weather.WeatherStation
import com.datastax.killrweather.WeatherStationId
import com.datastax.killrweather.controllers.WeatherStreamActor.WeatherUpdate
import com.datastax.killrweather.service.WeatherStationInfo
import org.scalatest.FunSuiteLike
import play.api.libs.json.Json

class WeatherStreamActorTest extends TestKit(ActorSystem("WeatherStreamActorTest")) with FunSuiteLike {

  import com.datastax.killrweather.controllers.Implicits._

  test("Send weather update") {
    val outActor = TestProbe()
    val apiActor = TestProbe()
    val stationId: WeatherStationId = WeatherStationId("station")
    val underTest = TestActorRef(new WeatherStreamActor(apiActor.ref, outActor.ref, stationId))
    val station = WeatherStation(stationId.id, "", "", "", 1.0, 2.0, 3.0)
    val info: WeatherStationInfo = WeatherStationInfo(station, Seq())

    underTest ! WeatherUpdate

    apiActor.expectMsg(GetWeatherStationWithPrecipitation(stationId))
    apiActor.reply(Some(info))
    outActor.expectMsg(s"""{"event":"weatherUpdate","data":${Json.toJson(info)}}""")
  }
}
