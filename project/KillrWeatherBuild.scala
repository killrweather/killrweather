/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import sbt._
import sbt.Keys._

object KillrWeatherBuild extends Build {
  import Settings._

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = parentSettings,
    aggregate = Seq(core, app, clients, examples)
  )

  lazy val core = Project(
    id = "core",
    base = file("./killrweather-core"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.core)
  )

  lazy val app = Project(
    id = "app",
    base = file("./killrweather-app"),
    dependencies = Seq(core),
    settings = defaultSettings ++ withSigar ++ Seq(libraryDependencies ++= Dependencies.app)
  ) configs IntegrationTest

  lazy val clients = Project(
    id = "clients",
    base = file("./killrweather-clients"),
    dependencies = Seq(core),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.client)
  )

  lazy val examples = Project(
    id = "examples",
    base = file("./killrweather-examples"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.examples)
  )

}

/** To use the connector, the only dependency required is:
  * "com.datastax.spark"  %% "spark-cassandra-connector" and possibly slf4j.
  * The others are here for other non-spark core and streaming code.
  */
object Dependencies {
  import Versions._

  object Compile {

    val akkaStream        = "com.typesafe.akka"   %% "akka-stream-experimental"          % "0.11"
    val akkaHttpCore      = "com.typesafe.akka"   %% "akka-http-core-experimental"       % "0.11"
    val akkaActor         = "com.typesafe.akka"   %% "akka-actor"   % Akka
    val akkaCluster       = "com.typesafe.akka"   %% "akka-cluster" % Akka
    val akkaRemote        = "com.typesafe.akka"   %% "akka-remote"  % Akka
    val akkaSlf4j         = "com.typesafe.akka"   %% "akka-slf4j"   % Akka
    val config            = "com.typesafe"        % "config"        % "1.2.1" force()

    val algebird          = "com.twitter"         %% "algebird-core"                      % Albebird
    val bijection         = "com.twitter"         %% "bijection-core"                     % Bijection
    val driver            = "com.datastax.cassandra" % "cassandra-driver-core"            % CassandraDriver exclude("com.google.guava", "guava") excludeAll(ExclusionRule("org.slf4j"))
    val jodaTime          = "joda-time"           % "joda-time"                           % JodaTime   % "compile;runtime"  // ApacheV2
    val jodaConvert       = "org.joda"            % "joda-convert"                        % JodaConvert % "compile;runtime"  // ApacheV2
    val json4sCore        = "org.json4s"          %% "json4s-core"                        % Json4s          // ApacheV2
    val json4sJackson     = "org.json4s"          %% "json4s-jackson"                     % Json4s          // ApacheV2
    val json4sNative      = "org.json4s"          %% "json4s-native"                      % Json4s          // ApacheV2
    val kafka             = "org.apache.kafka"    %% "kafka"                              % Kafka excludeAll(ExclusionRule("org.slf4j")) // ApacheV2
    val kafkaStreaming    = "org.apache.spark"    %% "spark-streaming-kafka"              % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2
    val logback           = "ch.qos.logback"      % "logback-classic"                     % Logback
    val scalazContrib     = "org.typelevel"       %% "scalaz-contrib-210"                 % ScalazContrib   // MIT
    val scalazContribVal  = "org.typelevel"       %% "scalaz-contrib-validation"          % ScalazContrib   // MIT
    val scalazStream      = "org.scalaz.stream"   %% "scalaz-stream"                      % ScalazStream    // MIT
    val slf4jApi          = "org.slf4j"           % "slf4j-api"                           % Slf4j % "provided"          // MIT
    val sparkML           = "org.apache.spark"    %% "spark-mllib"                        % Spark % "provided" exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core") // ApacheV2
    val sparkCatalyst     = "org.apache.spark"    %% "spark-catalyst"                     % Spark exclude("com.google.guava", "guava") exclude("org.apache.spark", "spark-core")
    val sparkCassandra    = "com.datastax.spark"  %% "spark-cassandra-connector"          % SparkCassandra // excludeAll(ExclusionRule("org.slf4j"))// ApacheV2
    val sparkCassandraEmb = "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % SparkCassandra excludeAll(ExclusionRule("org.apache.spark")) excludeAll(ExclusionRule("com.typesafe")) excludeAll(ExclusionRule("org.apache.cassandra")) excludeAll(ExclusionRule("com.datastax.cassandra")) //excludeAll(ExclusionRule("org.slf4j")) // ApacheV2
    val sigar             = "org.fusesource"      % "sigar"                               % Sigar
  }

  object Test {
    val akkaTestKit     = "com.typesafe.akka"     %% "akka-testkit"                       % SparkAkka    % "test,it" // ApacheV2
    val scalatest       = "org.scalatest"         %% "scalatest"                          % ScalaTest    % "test,it"
  }

  import Compile._

  val connector = Seq(driver, sparkCassandra, sparkCatalyst, sparkCassandraEmb)

  val json = Seq(json4sCore, json4sJackson, json4sNative)

  val scalaz = Seq(scalazContrib, scalazContribVal, scalazStream)

  val time = Seq(jodaConvert, jodaTime)

  val test = Seq(Test.akkaTestKit, Test.scalatest)

  /** Module deps */
  val client = Seq(akkaStream, akkaHttpCore, akkaActor, akkaCluster, akkaRemote, akkaSlf4j, logback, slf4jApi)

  val core = time

  val app = connector ++ json ++ scalaz ++ test ++ time ++
    Seq(algebird, bijection, kafka, kafkaStreaming, logback, slf4jApi, sparkML) ++
    Seq("com.typesafe.akka" %% "akka-cluster" % SparkAkka, "com.typesafe.akka" %% "akka-slf4j" % SparkAkka)// spark

  val examples = connector ++ time ++ json ++
    Seq(algebird, kafka, kafkaStreaming, logback, slf4jApi, sparkML)
}
