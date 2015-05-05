package com.datastax.killrweather

import akka.actor.{Actor, ActorLogging}
import akka.cluster.Cluster
import akka.util.Timeout
import com.datastax.killrweather.DashboardApiActor.GetWeatherStationWithPrecipitation
import com.datastax.killrweather.Weather.{DailyPrecipitation, WeatherStation}
import com.datastax.killrweather.WeatherEvent.{GetWeatherStations, GetTopKPrecipitation, GetWeatherStation}
import akka.pattern.{ ask, pipe}
import com.datastax.killrweather.service.{DayPrecipitation, WeatherStationInfo}
import play.Logger
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps


class DashboardApiActor extends Actor with ActorLogging {

  //todo: make this part of the request?
  val DefaultWeatherEvents = 50

  implicit val timeout = Timeout(5 seconds)
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  val guardian = context.actorSelection(Cluster(context.system).selfAddress
    .copy(port = Some(2550)) + "/user/node-guardian")


  override def receive: Receive = {
    case GetWeatherStationWithPrecipitation(wsid) =>
      val station = (guardian ? GetWeatherStation(wsid.id)).map(_.asInstanceOf[Some[WeatherStation]])
      val precipitation = (guardian ? GetTopKPrecipitation(wsid.id, DefaultWeatherEvents)).map(_.asInstanceOf[Seq[DailyPrecipitation]])

      station.flatMap {
        case Some(weatherStation) => precipitation.flatMap(precips => Future.successful(Some(
          WeatherStationInfo(weatherStation, precips.map(dp => DayPrecipitation(s"${dp.year}-${dp.month}-${dp.day}", dp.precipitation))))))
        case _ =>
          Logger.info(s"Unable to find station with ID $wsid")
          Future.successful(None)
      } pipeTo sender
    case request @ GetWeatherStations => guardian forward request
  }
}

object DashboardApiActor {
  case class GetWeatherStationWithPrecipitation(wsid: WeatherStationId)
}
