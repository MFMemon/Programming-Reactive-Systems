package protocols

import akka.actor.typed.*
import akka.actor.typed.scaladsl.*

import scala.concurrent.duration.*

object Transactor:

  sealed trait PrivateCommand[T] extends Product with Serializable
  final case class Committed[T](session: ActorRef[Session[T]], value: T) extends PrivateCommand[T]
  final case class RolledBack[T](session: ActorRef[Session[T]]) extends PrivateCommand[T]

  sealed trait Command[T] extends PrivateCommand[T]
  final case class Begin[T](replyTo: ActorRef[ActorRef[Session[T]]]) extends Command[T]

  sealed trait Session[T] extends Product with Serializable
  final case class Extract[T, U](f: T => U, replyTo: ActorRef[U]) extends Session[T]
  final case class Modify[T, U](f: T => T, id: Long, reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Commit[T, U](reply: U, replyTo: ActorRef[U]) extends Session[T]
  final case class Rollback[T]() extends Session[T]

  /**
    * @return A behavior that accepts public [[Command]] messages. The behavior
    *         would be wrapped in a [[SelectiveReceive]] decorator (with a capacity
    *         of 30 messages) so that beginning new sessions while there is already
    *         a currently running session is deferred to the point where the current
    *         session is terminated.
    * @param value Initial value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    */
  def apply[T](value: T, sessionTimeout: FiniteDuration): Behavior[Command[T]] =
      SelectiveReceive(30, idle(value, sessionTimeout).asInstanceOf[Behavior[Command[T]]])

  /**
    * @return A behavior that defines how to react to any [[PrivateCommand]] when the transactor
    *         has no currently running session.
    *         [[Committed]] and [[RolledBack]] messages would be ignored, and a [[Begin]] message
    *         would create a new session.
    *
    * @param value Value of the transactor
    * @param sessionTimeout Delay before rolling back the pending modifications and
    *                       terminating the session
    */
  private def idle[T](value: T, sessionTimeout: FiniteDuration): Behavior[PrivateCommand[T]] =
    Behaviors.receive{(ctx, msg) => msg match
      case Begin(replyTo) => 
        val sessionActor = ctx.spawnAnonymous{
          sessionHandler(value, ctx.self, Set.empty[Long])
        }
        ctx.scheduleOnce(sessionTimeout, ctx.self, RolledBack(sessionActor))
        ctx.watchWith(sessionActor, RolledBack(sessionActor))
        replyTo ! sessionActor
        inSession(value, sessionTimeout, sessionActor)
      
      case _ => Behaviors.same
    }  

  /**
    * @return A behavior that defines how to react to [[PrivateCommand]] messages when the transactor has
    *         a running session.
    *         [[Committed]] and [[RolledBack]] messages will commit and rollback the session, respectively.
    *         [[Begin]] messages would be unhandled (they will be handled by the [[SelectiveReceive]] decorator).
    *
    * @param rollbackValue Value to rollback to
    * @param sessionTimeout Timeout to use for the next session
    * @param sessionRef Reference to the child [[Session]] actor
    */
  private def inSession[T](rollbackValue: T, sessionTimeout: FiniteDuration, sessionRef: ActorRef[Session[T]]): Behavior[PrivateCommand[T]] =
    Behaviors.receive{(ctx, msg) => msg match
      case RolledBack(ref) if ref.compareTo(sessionRef) == 0 => 
        ctx.stop(sessionRef)
        idle(rollbackValue, sessionTimeout)
      case Committed(ref, value) if ref.compareTo(sessionRef) == 0 => 
        ctx.stop(sessionRef)
        idle(value, sessionTimeout)
      case Begin(_) => Behaviors.unhandled
      case _ => Behaviors.same
    }


  /**
    * @return A behavior handling [[Session]] messages. 
    * 
    * @param currentValue The session’s current value
    * @param commit Parent actor reference, to send the [[Committed]] message to
    * @param done Set of already applied [[Modify]] messages
    */
  private def sessionHandler[T](currentValue: T, commit: ActorRef[Committed[T]], done: Set[Long]): Behavior[Session[T]] =
      Behaviors.receive{(ctx, msg) => msg match
        case Extract(f, replyTo) => 
          try{
            replyTo ! f(currentValue)
            Behaviors.same
          }
          catch{
            case _: Exception => Behaviors.stopped
          }
        
        case Modify(f, id, reply, replyTo) => 
          try{
            val newVal = if done.contains(id) then currentValue else f(currentValue)
            replyTo ! reply
            sessionHandler(newVal, commit, done + id)
          }
          catch{
            case _: Exception => Behaviors.stopped
          }

        case Commit(reply, replyTo) => 
          replyTo ! reply
          commit ! Committed(ctx.self, currentValue)
          Behaviors.stopped

        case Rollback() => 
          Behaviors.stopped
      }

