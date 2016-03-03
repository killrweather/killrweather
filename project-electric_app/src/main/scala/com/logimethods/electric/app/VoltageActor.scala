/*
 * Copyright 2016 Logimethods - Laurent Magnin
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.logimethods.electric.app

import akka.actor.{ActorLogging, Actor}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import com.datastax.killrweather.{AggregationActor, Settings}
import com.logimethods.electric.Electric
import com.logimethods.electric.ElectricEvent

class VoltageActor(ssc: SparkContext, settings: Settings)
  extends AggregationActor with ActorLogging {
  import com.logimethods.electric.Electric._
  import com.logimethods.electric.ElectricEvent._
  
  def receive : Actor.Receive = {
    case GetVoltage(
      usagePointPK:Integer, 
      year:Integer,                     
      month:Byte, 
      day:Byte, 
      hour:Byte, 
      minute:Byte)        => voltage(usagePointPK, year, month, day, hour, minute)
  }
  
  def voltage(usagePointPK:Integer, year:Integer, month:Byte, day:Byte, hour:Byte, minute:Byte): Unit = {
    // TODO Complete
  }
      
}