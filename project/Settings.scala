/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import net.virtualvoid.sbt.graph.Plugin.graphSettings

import scala.language.postfixOps

object Settings extends Build {

  lazy val buildSettings = Seq(
    name := "KillrWeather.com",
    normalizedName := "killrweather",
    organization := "com.datastax.killrweather",
    organizationHomepage := Some(url("http://www.github.com/killrweather/killrweather")),
    scalaVersion := Versions.Scala,
    homepage := Some(url("https://github.com/killrweather/killrweather")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0")))
  )

  override lazy val settings = super.settings ++ buildSettings ++ Seq(shellPrompt := ShellPrompt.prompt)

  val parentSettings = buildSettings ++ Seq(
    publishArtifact := false,
    publish := {}
  )

  lazy val defaultSettings = testSettings ++ graphSettings ++ Seq(
    autoCompilerPlugins := true,
    libraryDependencies <+= scalaVersion { v => compilerPlugin("org.scala-lang.plugins" % "continuations" % v) },
    scalacOptions in (Compile, doc) ++= Seq("-implicits","-doc-root-content", "rootdoc.txt"),
    scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.JDK}", "-feature", "-language:_", "-deprecation", "-unchecked", "-Xlint"),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", Versions.JDK, "-target", Versions.JDK, "-Xlint:deprecation", "-Xlint:unchecked"),
    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false
    // have to force override of old from spark
    /*ivyXML := <dependencies>
      <exclude org="com.typesafe" module="config-1.0.2"/>
    </dependencies>*/
  )

  val tests = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

  val testOptionSettings = Seq(
    Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    Tests.Argument(TestFrameworks.JUnit, "-oDF", "-v", "-a")
  )

  lazy val testSettings = tests ++ Seq(
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    testOptions in Test ++= testOptionSettings,
    testOptions in IntegrationTest ++= testOptionSettings,
    fork in Test := true,
    fork in IntegrationTest := true,
    (compile in IntegrationTest) <<= (compile in Test, compile in IntegrationTest) map { (_, c) => c },
    managedClasspath in IntegrationTest <<= Classpaths.concat(managedClasspath in IntegrationTest, exportedProducts in Test)
  )

  lazy val withSigar = Seq(
    javaOptions in run ++= Seq("-Djava.library.path=./sigar", "-Xms128m", "-Xmx1024m")
  )

}

/**
 * TODO make plugin
 * Shell prompt which shows the current project, git branch
 */
object ShellPrompt {

  def gitBranches = ("git branch" lines_! devnull).mkString

  def current: String = """\*\s+([\.\w-]+)""".r findFirstMatchIn gitBranches map (_ group 1) getOrElse "-"

  def currBranch: String = ("git status -sb" lines_! devnull headOption) getOrElse "-" stripPrefix "## "

  lazy val prompt = (state: State) =>
    "%s:%s:%s> ".format("killrweather", Project.extract (state).currentProject.id, currBranch)

  object devnull extends ProcessLogger {
    def info(s: => String) {}
    def error(s: => String) {}
    def buffer[T](f: => T): T = f
  }
}