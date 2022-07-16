package kvstore

import akka.actor.{ Props, Terminated, ActorRef, Actor, Timers, ActorLogging}
import akka.pattern.{ ask, pipe, AskTimeoutException }
import scala.concurrent.Future
import akka.util.Timeout
import scala.concurrent.duration.*

object Replicator:
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case class SnapshotTimeout(req: Snapshot)

  def props(replica: ActorRef): Props = Props(Replicator(replica))

class Replicator(val replica: ActorRef) extends Actor with Timers with ActorLogging:
  import Replicator.*
  import context.*

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]

  // clients sending the Replicate request to the seq numnber allocated by this replicator to that request
  var activeClientsWithSeq = Map.empty[ActorRef, Long]

  // set of active clients requesting replication
  var activeClients = Set.empty[ActorRef]

  // set of all the Snapshots that are yet to send back the acknowledgments
  var pendingAcks = Set.empty[SnapshotAck]
  
  var _seqCounter = 0L
  def nextSeq() =
    val ret = _seqCounter
    _seqCounter += 1
    ret

  
  def receive: Receive =
    case msg @ Replicate(k, valOpt, id) =>
      val seq = nextSeq()
      val sReq = Snapshot(k, valOpt, seq)
      acks += seq -> (sender(), msg)
      activeClientsWithSeq += sender() -> seq
      replica ! sReq
      timers.startTimerWithFixedDelay(seq, SnapshotTimeout(sReq), Duration(200, MILLISECONDS))
      watch(sender())

    case SnapshotAck(k, seq) => 
      timers.cancel(seq)
      val seqOfAck = acks.get(seq)
      if seqOfAck.isDefined then
        val (client, request) = seqOfAck.get
        acks -= seq
        client ! Replicated(k, request.id)
        activeClientsWithSeq -= client
        unwatch(client)

    case Terminated(ref) => 
      val unwatchSeq = activeClientsWithSeq.get(ref)
      if unwatchSeq.isDefined then 
        timers.cancel(unwatchSeq.get)
        activeClientsWithSeq -= ref
        acks -= unwatchSeq.get

    case SnapshotTimeout(req) if acks.keySet.contains(req.seq) => replica ! req

    case _ => ()
