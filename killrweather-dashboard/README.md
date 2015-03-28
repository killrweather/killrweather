# Killr Dashboard


## Run

First ensure you have Cassandra running with the imported dataset as described in the top level [readme](https://github.com/killrweather/killrweather). You don't have to run any of the clients to import data as the dashboard can generate weather data.

Ensure you have the killrweather application running (```sbt app/run```). You should be seeing output like:

```
-------------------------------------------
Time: 1430214722500 ms
-------------------------------------------

-------------------------------------------
Time: 1430214723000 ms
-------------------------------------------

-------------------------------------------
Time: 1430214723500 ms
-------------------------------------------
```

To run the dashboard go into sbt console, switch to the dashboard project and execute run:

```
sbt
project dashboard
run
```

In future versions of Play you'll be able to run a subproject in dev mode without going into the console, see [here](https://github.com/playframework/playframework/issues/3484).

Then go to [localhost:9000](http://localhost:9000/).

You should see a list of weather stations:

![Weather Stations](https://raw.githubusercontent.com/chbatey/killrweather/wip-18-add-killrUi/killrweather-dashboard/docs/WeatherStations.png)

Click on a weather station to see its daily precipitation:

![Weather Station](https://raw.githubusercontent.com/chbatey/killrweather/wip-18-add-killrUi/killrweather-dashboard/docs/WeatherStation.png)

You can import static data as described [here](https://github.com/killrweather/killrweather) or use the load generation tool. Load generation will create a weather event per hour with a random hourly precipitation, you specify:

* Start date for the weather updates (defaults to one month ago)
* End date (defaults to now)
* Interval - how often to generate the events e.g every 250 ms

![Load Generation](https://raw.githubusercontent.com/chbatey/killrweather/wip-18-add-killrUi/killrweather-dashboard/docs/LoadGeneration.png)

Submitting will cause a weather event to be sent to Kafka for each interval. You can see the updates in real time by going back to the Weathert Station view:

![Weather Graph](https://raw.githubusercontent.com/chbatey/killrweather/wip-18-add-killrUi/killrweather-dashboard/docs/WeatherGraph.png)

