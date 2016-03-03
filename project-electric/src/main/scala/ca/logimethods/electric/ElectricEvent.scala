package ca.logimethods.electric

object ElectricEvent {
  import Electric._
  
  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait ElectricEvent extends Serializable

  sealed trait ElectricRequest extends ElectricEvent

  trait VoltageRequest extends ElectricRequest
  case class GetVoltageRequest(
      usagePointPK:Integer, 
      year:Integer,                     
      month:Byte, 
      day:Byte, 
      hour:Byte, 
      minute:Byte) extends VoltageRequest
  
}