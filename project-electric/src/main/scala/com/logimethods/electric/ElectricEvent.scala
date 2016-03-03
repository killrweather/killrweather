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

object ElectricEvent {
  import Electric._
  
  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait ElectricEvent extends Serializable

  sealed trait ElectricRequest extends ElectricEvent

  trait VoltageRequest extends ElectricRequest
  case class GetVoltage(
      usagePointPK:Integer, 
      year:Integer,                     
      month:Byte, 
      day:Byte, 
      hour:Byte, 
      minute:Byte) extends VoltageRequest
  
}