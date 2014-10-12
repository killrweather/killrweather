package com.datastax.killrweather.syntax

import org.json4s.{DefaultFormats, Formats}
import com.datastax.killrweather.api.WeatherCenterJson

object json {
  implicit val formats: Formats = WeatherCenterJson.formats(DefaultFormats)
}
