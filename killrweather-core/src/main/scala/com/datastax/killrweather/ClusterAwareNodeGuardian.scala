package com.datastax.killrweather

import java.util.concurrent.TimeoutException

import scala.concurrent.duration._
import akka.actor._
import akka.pattern.gracefulStop
import akka.util.Timeout
import com.datastax.killrweather.WeatherEvent.{NodeInitialized, OutputStreamInitialized}

/** A `NodeGuardian` is the root of each KillrWeather deployed application, where
 * any special application logic is handled in its implementers, but the cluster
 * work and node lifecycle and supervision events are handled here.
 *
 * It extends [[ClusterAware]] which handles creation of the [[akka.cluster.Cluster]],
 * but does the cluster.join and cluster.leave itself.
 *
 * `NodeGuardianLike` also handles graceful shutdown of the node and all child actors. */
abstract class ClusterAwareNodeGuardian extends ClusterAware with ActorLogging {
  import SupervisorStrategy._
  // TODO customize
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      case _: ActorInitializationException => Stop
      case _: IllegalArgumentException => Stop
      case _: IllegalStateException    => Restart
      case _: TimeoutException         => Escalate
      case _: Exception                => Escalate
    }

  override def preStart(): Unit = {
    log.info("Starting at {}", cluster.selfAddress)
    cluster.join(cluster.selfAddress)
  }

  override def postStop(): Unit = {
    log.info("Node {} shutting down.", cluster.selfAddress)
    cluster.leave(self.path.address)
    gracefulShutdown()
  }

  /** On startup, actor is in an [[uninitialized]] state. */
  override def receive = uninitialized orElse initialized orElse super.receive

  /** When [[OutputStreamInitialized]] is received the actor moves from
    * [[uninitialized]] to an `initialized` state with [[ActorContext.become()]].
    */
  def uninitialized: Actor.Receive = {
    case OutputStreamInitialized => initialize()
  }

  def initialize(): Unit = {
    log.info(s"Node is transitioning from 'uninitialized' to 'initialized'")
    context.system.eventStream.publish(NodeInitialized)
  }

  /** Must be implemented by an Actor. */
  def initialized: Actor.Receive

  def gracefulShutdown(): Unit = {
    val timeout = Timeout(5.seconds)
    context.children foreach (gracefulStop(_, timeout.duration))
    log.info(s"Graceful shutdown completed.")
  }

}
/*class NodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka, settings: WeatherSettings)
  extends ClusterAware with AggregationActor with Assertions with ActorLogging {


  /** On startup, actor is in an [[uninitialized]] state. */
  override def receive = uninitialized orElse initialized orElse super.receive

  /** When [[OutputStreamInitialized]] is received from the [[KafkaStreamingActor]] after
    * it creates and defines the [[KafkaInputDStream]], at which point the streaming
    * checkpoint can be set, the [[StreamingContext]] can be started, and the actor
    * moves from [[uninitialized]] to [[initialized]]with [[ActorContext.become()]].
    */
  def uninitialized: Actor.Receive = {
    case OutputStreamInitialized => initialize()
  }

  def initialized: Actor.Receive = {
    case e: KafkaMessageEnvelope[_,_] =>
      //publisher forward e
    case e: TemperatureRequest =>
      temperature forward e
    case e: PrecipitationRequest =>
      precipitation forward e
    case e: WeatherStationRequest =>
      station forward e
    case PoisonPill =>
      gracefulShutdown()
  }


}
*/