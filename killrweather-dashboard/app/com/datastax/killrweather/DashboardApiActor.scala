package com.datastax.killrweather

import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.util.Timeout
import akka.pattern.{ ask, pipe}
import com.datastax.killrweather.Weather.{DailyPrecipitation, WeatherStation}
import com.datastax.killrweather.WeatherEvent._
import com.datastax.killrweather.service.{DayPrecipitation, WeatherStationInfo}
import scala.concurrent.duration._
import scala.language.postfixOps


class DashboardApiActor extends Actor with ActorLogging with DashboardProperties with PrecipitationRequest {

  implicit val timeout = Timeout(5 seconds)
  import context.dispatcher

  val guardian = context.actorSelection(Cluster(context.system).selfAddress
    .copy(port = Some(2550)) + "/user/node-guardian")


  def receive: Receive = {
    case GetWeatherStationWithPrecipitation(wsid) => getWeatherStationPrecipitation(wsid)
    case request @ GetWeatherStations => guardian forward request
  }

  def getWeatherStationPrecipitation(wsid: WeatherStationId): Unit = {
    val dailyPrecipitation = (guardian ? GetTopKPrecipitation(wsid, WeatherEvents)).mapTo[Seq[DailyPrecipitation]]
    val future = for {
      station <- (guardian ? GetWeatherStation(wsid)).mapTo[Option[WeatherStation]]
      precipitation: Seq[DailyPrecipitation] <- dailyPrecipitation
    } yield {
      val daily: Seq[DayPrecipitation] = for (dp <- precipitation) yield DayPrecipitation(s"${dp.year}-${dp.month}-${dp.day}", dp.precipitation)
      station.map(WeatherStationInfo(_, daily))
    }
    future pipeTo sender()
  }
}