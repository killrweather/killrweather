package com.datastax.killrweather.infrastructure

import com.datastax.driver.core.{Session, Cluster}
import com.datastax.killrweather.Weather.{DailyPrecipitation, AnnualPrecipitation, WeatherStation}
import com.datastax.killrweather.WeatherStationId
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, BeforeAndAfter, BeforeAndAfterAll, FunSuite}

import scala.concurrent.Future

//todo have a look at phantom
class WeatherStationDaoTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with ScalaFutures with Matchers {

  var cluster: Cluster = _
  var session: Session = _

  override protected def beforeAll(): Unit = {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()
    cluster = Cluster.builder().addContactPoint("localhost").withPort(9142).build()
    session = cluster.connect()
    session.execute("DROP KEYSPACE IF EXISTS isd_weather_data_test")
    session.execute("CREATE KEYSPACE isd_weather_data_test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("USE isd_weather_data_test")
    session.execute("CREATE TABLE weather_station (id text PRIMARY KEY, name text, country_code text, state_code text, call_sign text, lat double, long double, elevation double)")
    session.execute("CREATE TABLE year_cumulative_precip ( wsid text, year int, precipitation counter, PRIMARY KEY ((wsid), year)) WITH CLUSTERING ORDER BY (year DESC)")
    session.execute("CREATE TABLE daily_aggregate_precip ( wsid text, year int, month int, day int, precipitation counter, PRIMARY KEY ((wsid), year, month, day)) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)")
  }

  override protected def afterAll(): Unit = {
    cluster.close()
  }

  before {
    session.execute("TRUNCATE weather_station")
    session.execute("TRUNCATE year_cumulative_precip")
    session.execute("TRUNCATE daily_aggregate_precip")
  }


  test("weather_station: Retrieve all ") {
    session.execute("INSERT INTO weather_station (id, name, country_code , state_code , call_sign , lat , long , elevation ) " +
      "values ('1234', 'Name', 'GB', 'LON', 'BBB', 1.0, 2.0, 3.0 )")
    val underTest = new WeatherStationDao(session)

    val stations = underTest.retrieveWeatherStations()

    whenReady(stations) { result =>
      result should equal(Seq(WeatherStation("1234", "Name", "GB", "BBB", 1.0, 2.0, 3.0)))
    }
  }

  test("weather_station: Retrieve single ") {
    session.execute("INSERT INTO weather_station (id, name, country_code , state_code , call_sign , lat , long , elevation ) " +
      "values ('1234', 'Name', 'GB', 'LON', 'BBB', 1.0, 2.0, 3.0 )")
    val underTest = new WeatherStationDao(session)

    val stations = underTest.retrieveWeatherStation(WeatherStationId("1234"))

    whenReady(stations) { result =>
      result should equal(Some(WeatherStation("1234", "Name", "GB", "BBB", 1.0, 2.0, 3.0)))
    }
  }

  test("weather_station: Retrieve single that does not exist") {
    val underTest = new WeatherStationDao(session)

    val stations = underTest.retrieveWeatherStation(WeatherStationId("1234"))

    whenReady(stations) { result =>
      result should equal(None)
    }
  }

  test("year_cumulative_precip: Retrieve") {
    session.execute("update year_cumulative_precip SET precipitation = precipitation + 10 WHERE wsid ='1234' and year = 2015")
    session.execute("update year_cumulative_precip SET precipitation = precipitation + 15 WHERE wsid ='1234' and year = 2014")
    session.execute("update year_cumulative_precip SET precipitation = precipitation + 5 WHERE wsid ='5678' and year = 2015")
    val underTest = new WeatherStationDao(session)

    val yearlyPrecipFor1234 : Future[AnnualPrecipitation] = underTest.retrieveYearlyPrecipitation(WeatherStationId("1234"), 2015)

    whenReady(yearlyPrecipFor1234) { result =>
      result should equal(AnnualPrecipitation("1234", 2015, 10.0))
    }
  }

  test("daily_aggregate_precip: Retrieve") {
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 10 where wsid = '1234' and year = 2015 and month = 03 and day = 30")
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 20 where wsid = '1235' and year = 2014 and month = 03 and day = 30")
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 30 where wsid = '1235' and year = 2015 and month = 02 and day = 30")
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 40 where wsid = '1235' and year = 2015 and month = 03 and day = 29")

    val underTest = new WeatherStationDao(session)

    val dailyPrecipFor1234 : Future[DailyPrecipitation] = underTest.retrieveDailyPrecipitation(WeatherStationId("1234"), 2015, 3, 30)

    whenReady(dailyPrecipFor1234) { result =>
      result should equal(DailyPrecipitation("1234", 2015, 3, 30, 10.0))
    }
  }

  test("daily_aggregate_precip: Retrieve all with limit") {
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 10 where wsid = '1234' and year = 2015 and month = 03 and day = 30")
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 40 where wsid = '1234' and year = 2015 and month = 03 and day = 29")
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 30 where wsid = '1234' and year = 2015 and month = 02 and day = 30")
    session.execute("update daily_aggregate_precip set precipitation = precipitation + 20 where wsid = '1234' and year = 2014 and month = 03 and day = 30")

    val underTest = new WeatherStationDao(session)

    val dailyPrecipFor1234 = underTest.retrieveDailyPrecipitation(WeatherStationId("1234"), 3)

    whenReady(dailyPrecipFor1234) { result =>
      result should equal(Seq(
        DailyPrecipitation("1234", 2015, 3, 30, 10.0),
        DailyPrecipitation("1234", 2015, 3, 29, 40.0),
        DailyPrecipitation("1234", 2015, 2, 30, 30.0)
      ))
    }
  }
}
