package ca.logimethods.electric

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
                       
  case class UsagePointData(
      usagePointPK:Integer, 
      year:Integer,                     
      month:Byte, 
      day:Byte, 
      hour:Byte, 
      minute:Byte, 
      epoch:Long, 
      dayInWeek:Byte, 
      demand:Float, 
      voltage:Float) extends ElectricModel

}