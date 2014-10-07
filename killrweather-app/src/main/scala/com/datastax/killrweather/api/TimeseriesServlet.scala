package com.datastax.killrweather.api

import javax.servlet.http.HttpServletRequest

import com.datastax.killrweather.api.WeatherServlet
import org.scalatra._
import org.joda.time.{DateTimeZone, DateTime}
import org.json4s.{Formats, DefaultFormats}

class TimeseriesServlet extends WeatherServlet {
  import WeatherApi._

  override def jsonFormats: Formats = apiFormats

  def dayOfYearParam(params: Params): Int = params.get("dayofyear").map(_.toInt) getOrElse currentDayOfYear

  def zipcodeParam(params: Params): Option[Int] = params.get("zipcode").map(_.toInt)

  protected def stationIdOrHalt(request: HttpServletRequest, errorBody: => String => Any = identity): WeatherStationId = {
    WeatherStationId(request) getOrElse halt(status = 400, body = errorBody("No Station ID")) valueOr (fail => halt(status = 400, body = errorBody(fail)))
  }

  private def currentDayOfYear: Int = new DateTime(DateTimeZone.UTC).dayOfYear().get()
}