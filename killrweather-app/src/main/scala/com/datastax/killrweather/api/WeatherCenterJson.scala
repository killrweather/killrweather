package com.datastax.killrweather.api

import java.util.UUID

import org.json4s._

object WeatherCenterJson {
  import com.datastax.killrweather.WeatherEvents._
  import com.datastax.killrweather.Weather._

  lazy val formats: Formats => Formats =
    _ + uuidSerializer + hints

  lazy private val hints = FullTypeHints(List(
    classOf[GetWeatherStation], classOf[TemperatureAggregate]
  ))

  protected object uuidSerializer extends CustomSerializer[UUID](format => (
    { case JString(uuid) => UUID.fromString(uuid) },
    { case uuid: UUID => JString(uuid.toString) }))
}
