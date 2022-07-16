package kvstore

import akka.actor.{ OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, DeadLetter, ActorLogging, Timers, actorRef2Scala }
import kvstore.Arbiter.*
import akka.pattern.{ ask, pipe, AskTimeoutException }
import scala.concurrent.duration.*
import scala.concurrent.Future
import akka.util.Timeout
import java.time.LocalDateTime.*

object Replica:
  sealed trait Operation:
    def key: String
    def id: Long
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  // all the properties of the update request including the requesting client's ActorRef.
  case class UpdateProperties(client: ActorRef, key: String, value: Option[String], keySeq: Long)

  // message constructs for communication between prinary replica and the helper UpdateHandler actor
  case class PersistAndReplicate(key: String, valueOption: Option[String], id: Long)
  case class PersistedAndReplicated(id: Long)

  // message sent to all the activeUpdateHandlers with the updated list of replicators in the event of replicas leaving the cluster
  case class Replicators(replicators: Set[ActorRef])

  // message to be sent to the same replica at the timeout of 1 second
  case class UpdateTimeout(client: ActorRef, updateHandler: ActorRef, id: Long)

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(Replica(arbiter, persistenceProps))

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging with Timers:
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.*

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  
  // the map of UpdateHandlers and their corresponding timer keys
  var activeTimers = Map.empty[ActorRef, String]


  // a map from active updateHandler ActorRefs to the updateProperties objects.
  var activeUpdateHandlers = Map.empty[ActorRef, UpdateProperties]
  
  // map from keys to their update request sequences
  var keyUpdateReqSeq = Map.empty[String, Long].withDefaultValue(0L)

  // map from keys to their sequences of persistent updates.
  var keyUpdatedSeq = Map.empty[String, Long].withDefaultValue(0L)

  val withPersistence = !(persistenceProps.args.contains(false))



  arbiter ! Join


  def receive =
    case JoinedPrimary   => become(leader)
    case JoinedSecondary => become(replica(0L))
    case _ => ()


  def leader: Receive =

    case Insert(k, v, id) => 
      val newUpdateHandler = actorOf(UpdateHandler.props(replicators, persistenceProps))
      val keySeq = keyUpdateReqSeq.getOrElse(k, 0L) + 1L
      keyUpdateReqSeq += k -> keySeq
      val client = sender()
      activeUpdateHandlers += newUpdateHandler -> UpdateProperties(client, k, Some(v), keySeq)
      newUpdateHandler ! PersistAndReplicate(k, Some(v), id)
      val timerKey = now().toString
      timers.startTimerWithFixedDelay(timerKey, UpdateTimeout(client, newUpdateHandler, id), Duration(1, SECONDS))
      activeTimers += newUpdateHandler -> timerKey

    case Remove(k, id) => 
      val newUpdateHandler = actorOf(UpdateHandler.props(replicators, persistenceProps))
      val keySeq = keyUpdateReqSeq.getOrElse(k, 0L) + 1L
      keyUpdateReqSeq += k -> keySeq
      val client = sender()
      activeUpdateHandlers += newUpdateHandler -> UpdateProperties(client, k, None, keySeq)
      newUpdateHandler ! PersistAndReplicate(k, None, id)
      val timerKey = now().toString
      timers.startTimerWithFixedDelay(now().toString, UpdateTimeout(client, newUpdateHandler, id), Duration(1, SECONDS))
      activeTimers += newUpdateHandler -> timerKey

    case Get(k, id) => 
      sender() ! GetResult(k, kv.get(k), id)

    case Replicas(replicas) => 
      val registeredReplicas = secondaries.keySet
      val leftReplicas = registeredReplicas.diff(replicas)
      if !leftReplicas.isEmpty then
        for r <- leftReplicas
        do
          val orphanReplicator = secondaries.get(r).get
          replicators -= orphanReplicator
          secondaries -= r
          orphanReplicator ! PoisonPill
        activeUpdateHandlers.keySet.foreach(_ ! Replicators(replicators))
      else
        val newReplicas = replicas.diff(registeredReplicas) - self
        newReplicas.flatMap{r =>
          val newReplicator = actorOf(Replicator.props(r))
          secondaries += r -> newReplicator
          replicators += newReplicator
          kv.map(tup => newReplicator ! Replicate(tup._1, Some(tup._2), keyUpdatedSeq.get(tup._1).get))
        }
    
    case PersistedAndReplicated(id) => 
      val updateProperties = activeUpdateHandlers.get(sender()).get
      val key = updateProperties.key
      val newKeySeq = updateProperties.keySeq
      if newKeySeq > keyUpdatedSeq.getOrElse(key, 0L) then
        keyUpdatedSeq += key -> newKeySeq
        updateProperties.value match {
          case Some(v) => kv += key -> v
          case None => kv -= key
        }
      activeUpdateHandlers -= sender()
      timers.cancel(activeTimers.get(sender()).get)
      sender() ! PoisonPill
      updateProperties.client ! OperationAck(id)
    
    case UpdateTimeout(client, updateHandler, id) if activeUpdateHandlers.keySet.contains(updateHandler) => 
      client ! OperationFailed(id)
      activeUpdateHandlers -= updateHandler
      updateHandler ! PoisonPill
      timers.cancel(activeTimers.get(updateHandler).get)

    case _ => ()



  def replica(seq: Long): Receive =
    case Get(k, id) => 
      sender() ! GetResult(k, kv.get(k), id)
    
    case Snapshot(k, valOpt, s) if s < seq => 
      sender() ! SnapshotAck(k, s)
    
    case Snapshot(k, valOpt, s) if s > seq => ()
    
    case Snapshot(k, valOpt, s) => 
      if valOpt.isDefined then kv += k -> valOpt.get else kv -= k
      val newUpdateHandler = actorOf(UpdateHandler.props(replicators, persistenceProps))
      val client = sender()
      activeUpdateHandlers += newUpdateHandler -> UpdateProperties(client, k, valOpt, s)
      newUpdateHandler ! PersistAndReplicate(k, valOpt, s)
      val timerKey = now().toString
      timers.startTimerWithFixedDelay(timerKey, UpdateTimeout(client, newUpdateHandler, s), Duration(800, MILLISECONDS))
      activeTimers += newUpdateHandler -> timerKey
      context.become(replica(seq + 1))

    case PersistedAndReplicated(id) =>
      val updateProperties = activeUpdateHandlers.get(sender()).get
      val key = updateProperties.key
      activeUpdateHandlers -= sender()
      sender() ! PoisonPill
      updateProperties.client ! SnapshotAck(updateProperties.key, updateProperties.keySeq)
      timers.cancel(activeTimers.get(sender()).get)
    
    case UpdateTimeout(_, updateHandler, _) if activeUpdateHandlers.keySet.contains(updateHandler) =>
      activeUpdateHandlers -= updateHandler
      updateHandler ! PoisonPill
      timers.cancel(activeTimers.get(updateHandler).get)



