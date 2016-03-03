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

package com.logimethods.electric

object Electric {

  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait ElectricModel extends Serializable
  
  case class UsagePoint(
      usagePointPK:Integer, 
      year:Integer,                     
      month:Byte, 
      day:Byte,
      line:Integer, 
      lineStr:String, 
      pt:Integer, 
      ptStr:String, 
      usagePointPKStr:String) extends ElectricModel
                       
  case class RawUsagePointData(
      usagePointPK:Integer, 
      year:Integer,                     
      month:Byte, 
      day:Byte, 
      hour:Byte, 
      minute:Byte, 
//      epoch:Long, 
//      dayInWeek:Byte, 
      demand:Float, 
      voltage:Float) extends ElectricModel
      
  object RawUsagePointData {
    /** Tech debt - don't do it this way ;) */
    def apply(array: Array[String]): RawUsagePointData = {
      RawUsagePointData(
        usagePointPK = array(0).toInt,
        year = array(1).toInt,
        month = array(2).toByte,
        day = array(3).toByte,
        hour = array(4).toByte,
        minute = array(5).toByte,
        demand = array(6).toFloat,
        voltage = array(7).toFloat)
    }
  }
}