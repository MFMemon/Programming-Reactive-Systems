package kvstore

import akka.actor.{OneForOneStrategy, PoisonPill, Props, SupervisorStrategy, Terminated, ActorRef, Actor, Timers, ActorLogging}
import scala.concurrent.duration.*
import akka.pattern.{ ask, pipe, AskTimeoutException }
import akka.util.Timeout



object UpdateHandler:

  case class PersistenceTimeout(persistMsg: Persistence.Persist)

  def props(replicators: Set[ActorRef], persistenceProps: Props): Props = Props(UpdateHandler(replicators, persistenceProps))


class UpdateHandler(replicators: Set[ActorRef], persistenceProps: Props) extends Actor with Timers with ActorLogging:

  import UpdateHandler.*
  import Replica.*
  import Replicator.*
  import Persistence.*
  import context.*
  
  var replicatorsAwaited = replicators

  val persistenceActor = actorOf(persistenceProps)
  
  var id = Option.empty[Long]

  override def supervisorStrategy = OneForOneStrategy(withinTimeRange = Duration(900, MILLISECONDS)){
      case e: Exception => SupervisorStrategy.Resume
  }

  def switchContext(replicated: Boolean, persisted: Boolean) = (replicated, persisted) match {
      case (true, true) => parent ! PersistedAndReplicated(id.get)
      case _ => become(updating(replicated, persisted))
  }

  def receive = if replicators.isEmpty then updating(true, false) else updating(false, false)

  def updating(replicated: Boolean, persisted: Boolean) : Receive = 
    case PersistAndReplicate(k, valOpt, id) => 
      val persistMsg = Persist(k, valOpt, id)
      this.id = Some(id)
      replicators.foreach{r =>
        r ! Replicate(k, valOpt, id)
      }
      persistenceActor ! persistMsg
      timers.startTimerWithFixedDelay("1", PersistenceTimeout(persistMsg), Duration(75, MILLISECONDS))

    case Replicated(k, id) => 
      if (replicatorsAwaited - sender()).isEmpty then
        switchContext(true, persisted)
      else
        replicatorsAwaited -= sender()

    case Replicators(replicators) => 
      if replicators.isEmpty then switchContext(true, persisted) else replicatorsAwaited = replicators

    case Persisted(_, _) =>
        switchContext(replicated, true)

    case PersistenceTimeout(persistMsg) => persistenceActor ! persistMsg

    case _ => ()


