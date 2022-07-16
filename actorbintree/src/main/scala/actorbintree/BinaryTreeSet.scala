/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor.*
import scala.collection.immutable.Queue

object BinaryTreeSet:

  trait Operation:
    def requester: ActorRef
    def id: Int
    def elem: Int

  trait OperationReply:
    def id: Int

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request the tree for the node that will be the new root. The new root will
    * be used to copy the existing tree to new tree in the event of GC. 
    */
  
  case class searchRoot(req: ActorRef)

  /** Holds the answer to the searchRoot request.
    */

  case class rootFound(elem: Int)

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply



class BinaryTreeSet extends Actor:
  import BinaryTreeSet.*
  import BinaryTreeNode.*

  def createRoot(elem: Int): ActorRef = context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
  val dummyActor = Option(ActorRef.noSender)

  // used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  def receive = normal(dummyActor)

  /** Accepts `Operation` and `GC` messages. */
  def normal(root: Option[ActorRef]): Receive = {  
    case op: Operation if root.isDefined => root.get ! op
    case Insert(req, id, elem) => 
      req ! OperationFinished(id)
      context.become(
        normal(Some(createRoot(elem))), 
        true
      )
    case Remove(req, id, elem) => req ! OperationFinished(id)
    case Contains(req, id, elem) => req ! ContainsResult(id, false)
    case GC if root.isDefined =>
      root.get ! searchRoot(self)
      context.become(
      garbageCollecting(root, dummyActor), 
      true)
    case _ => ()
   }

  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(oldRoot: Option[ActorRef], newRoot: Option[ActorRef]): Receive = {
    case GC => ()
    case rootFound(elem) if !newRoot.isDefined  => 
      val newRootSome = Some(createRoot(elem))
      oldRoot.get ! CopyTo(newRootSome.get)
      context.become(
        garbageCollecting(oldRoot, newRootSome), 
        true
      )
    case op: Operation => 
      pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished => 
      context.stop(oldRoot.get)
      def dequeOps(q: Queue[Operation]): Unit = 
        if !q.isEmpty then
          val (op, newQ) = q.dequeue
          pendingQueue = newQ
          newRoot.get ! op
          dequeOps(pendingQueue)
      dequeOps(pendingQueue)
      context.become(normal(newRoot), true)
    case _ => ()
  }

object BinaryTreeNode:
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  /**
   * Acknowledges that a copy has been completed. This message should be sent
   * from a node to its parent, when this node and all its children nodes have
   * finished being copied.
   */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor:
  import BinaryTreeNode.*
  import BinaryTreeSet.*

  var subtrees = scala.collection.immutable.Map[Position, ActorRef]()
  var removed = initiallyRemoved

  def receive = normal

  def leftNodeRef = subtrees.get(Left)
  def rightNodeRef = subtrees.get(Right)
  def subtreesRefs = subtrees.values.toSet


  def elemMatched(reqElem: Int, nodeElem: Int) = true match{
    case true if reqElem == nodeElem => 0
    case true if reqElem > nodeElem => 1
    case true if reqElem < nodeElem => -1
    case _ => Int.MaxValue
  }     

  def operationHandler(op: Operation) = 

    val elem = op.elem

    (op, elemMatched(elem, this.elem)) match{

      case (Insert(req, id, _), 0) => 
        removed = false
        req ! OperationFinished(id)
      case (Insert(req, id, _) , -1) if !(leftNodeRef.isDefined) => 
        subtrees += Left -> context.actorOf(props(elem, false))
        req ! OperationFinished(id)
      case (Insert(req, id, _) , 1) if !(rightNodeRef.isDefined) => 
        subtrees += Right -> context.actorOf(props(elem, false))
        req ! OperationFinished(id)

      case (Remove(req, id, _), 0) => 
        removed = true
        req ! OperationFinished(id)
      case (Remove(req, id, _) , -1) if !(leftNodeRef.isDefined) => req ! OperationFinished(id)
      case (Remove(req, id, _) , 1) if !(rightNodeRef.isDefined) => req ! OperationFinished(id)

      case (Contains(req, id, _), 0) => req ! ContainsResult(id, !this.removed)
      case (Contains(req, id, _), -1) if !(leftNodeRef.isDefined) => req ! ContainsResult(id, false)
      case (Contains(req, id, _), 1) if !(rightNodeRef.isDefined) => req ! ContainsResult(id, false)
     
      case (_ , -1) if leftNodeRef.isDefined => leftNodeRef.get ! op
      case (_ , 1) if rightNodeRef.isDefined => rightNodeRef.get ! op

    }    

  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case op: Operation => operationHandler(op)
    case msg @ searchRoot(req) => 
      if !this.removed then 
        req ! rootFound(this.elem)
      else subtreesRefs.foreach(_ ! msg)
    case msg @ CopyTo(newRoot) => 
      subtreesRefs.foreach(_ ! msg)
      if !removed then 
        newRoot ! Insert(self, elem*2, elem)
        context.become(copying(subtreesRefs + self), true)
      else
        self ! OperationFinished(elem*2)
        context.become(copying(subtreesRefs), true)
      
  }

  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef]): Receive = 
    case OperationFinished(_) =>
      val remaining = expected - self
      if remaining.isEmpty then context.parent ! CopyFinished
      else context.become(copying(remaining), true)
    case CopyFinished => 
      val remaining = expected - sender()
      if remaining.isEmpty then context.parent ! CopyFinished
      else context.become(copying(remaining), true)
    


