package followers

import akka.NotUsed
import akka.event.Logging
import akka.stream.scaladsl.{BroadcastHub, Flow, Framing, Keep, MergeHub, Sink, Source}
import akka.stream.{ActorAttributes, Materializer, Attributes, Inlet, Outlet, FlowShape}
import akka.util.ByteString
import followers.model.{Event, Followers, Identity}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Success, Failure}

/**
  * Utility object that describes stream manipulations used by the server
  * implementation.
  */
object Server extends ServerModuleInterface:

  case object EndOfStream

  /**
    * A flow that consumes chunks of bytes and produces `String` messages.
    *
    * Each incoming chunk of bytes doesn’t necessarily contain ''exactly one'' message
    * payload (it can contain fragments of payloads only). Therefore it processes these
    * chunks to produce ''frames'' containing exactly one message payload.
    *
    * Messages are delimited by the '\n' character.
    */
  val reframedFlow: Flow[ByteString, String, NotUsed] =
    Flow[ByteString].via(Framing.delimiter(ByteString("\n"), 256, false)).map(_.utf8String)

  /**
    * A flow that consumes chunks of bytes, parses those and produces [[Event]] messages.
    */
  val eventParserFlow: Flow[ByteString, Event, NotUsed] =
    reframedFlow.map(Event.parse)

  /**
    * Implements a Sink that will look for the first [[Identity]]
    * (we expect there will be only one), from a stream of commands and materializes a Future with it.
    *
    * Subsequent values once we found an identity will be ignored.
    */
  val identityParserSink: Sink[ByteString, Future[Identity]] =
    reframedFlow
      .map(Identity.parse)
      .toMat(Sink.head)(Keep.right)

  /**
    * A flow that consumes unordered messages and produces messages ordered by `sequenceNr`.
    *
    * User clients expect to be notified of events in the correct order, regardless of the order in which the
    * event source sent them.
    *
    * It buffers messages with a higher sequence number than the next one expected to be
    * produced. The first message to produce has a sequence number of 1.
    */
  val reintroduceOrdering: Flow[Event, Event, NotUsed] =
    given [T <: Event] : Ordering[T] = Ordering.by(_.sequenceNr)
    
    Flow[Event].statefulMapConcat{ () =>
      var nextSeqN = 1
      var eventSet = SortedSet.empty[Event]

      def nextEvents : List[Event] =
        val headEvent = eventSet.head
        val eventList = eventSet.toList
        if headEvent.sequenceNr.compare(nextSeqN) != 0 then
          Nil
        else
          eventSet = SortedSet(eventList.tail*)
          nextSeqN += 1
          headEvent :: (if eventSet.isEmpty then Nil else nextEvents)

      (event) =>
        eventSet += event
        nextEvents

    }

  /**
    * A flow that associates a state of [[Followers]] to
    * each incoming [[Event]].
    */
  val followersFlow: Flow[Event, (Event, Followers), NotUsed] = 
    import Event._

    Flow[Event]
      .statefulMapConcat{ () => 
        var followersMap : Followers = Map.empty[Int, Set[Int]]
        
        (event: Event) => event match
          case Follow(seq, from, to) => 
            val followerOption = followersMap.get(from)
            if followerOption.isDefined then 
              followersMap += from -> (followerOption.get + to)
            else 
              followersMap += from -> Set(to)
              
          case Unfollow(seq, from, to) => 
            val followerOption = followersMap.get(from)
            followersMap += from -> (followerOption.get - to)

          case _ => ()

          (event, followersMap) :: Nil
      }


  /**
    * @return Whether the given user should be notified by the incoming `Event`,
    *         given the current state of `Followers`. 
    * @param userId Id of the user
    * @param eventAndFollowers Event and current state of followers
    */
  def isNotified(userId: Int)(eventAndFollowers: (Event, Followers)): Boolean =
    import Event._
    val (event, followersMap) = eventAndFollowers
    event match
      case Follow(_, _, to) => to.compare(userId) == 0
      case PrivateMsg(_, _, to) => to.compare(userId) == 0
      case Unfollow(seq, from, to) => false
      case Broadcast(_) => true
      case StatusUpdate(seq, from) => 
        val currentFollowers = followersMap.get(userId)
        currentFollowers.isDefined && currentFollowers.get.contains(from)
      

  // Utilities to temporarily have unimplemented parts of the program
  private def unimplementedFlow[A, B, C]: Flow[A, B, C] =
    Flow.fromFunction[A, B](_ => ???).mapMaterializedValue(_ => ??? : C)

  private def unimplementedSink[A, B]: Sink[A, B] = Sink.ignore.mapMaterializedValue(_ => ??? : B)

/**
  * Creates a hub accepting several client connections and a single event connection.
  *
  * @param executionContext Execution context for `Future` values transformations
  * @param materializer Stream materializer
  */
class Server(using ExecutionContext, Materializer)
  extends ServerInterface with ExtraStreamOps:
  import Server.*

  /**
    * The hub is instantiated here. It allows new user clients to join afterwards
    * and receive notifications from their event feed (see the `clientFlow()` member
    * below). The hub also allows new events to be pushed to it (see the `eventsFlow`
    * member below).
    *
    * The following expression creates the hub and returns a pair containing:
    *  1. A `Sink` that consumes events data,
    *  2. and a `Source` of decoded events paired with the state of followers.
    */
  val (inboundSink, broadcastOut) =
    /**
      * A flow that consumes the event source, re-frames it,
      * decodes the events, re-orders them, and builds a Map of
      * followers at each point it time. It produces a stream
      * of the decoded events associated with the current state
      * of the followers Map.
      */
    val incomingDataFlow: Flow[ByteString, (Event, Followers), NotUsed] =
      Flow[ByteString]
        .via(eventParserFlow)
        .via(reintroduceOrdering)
        .via(followersFlow)

    // Wires the MergeHub and the BroadcastHub together and runs the graph
    MergeHub.source[ByteString](256)
      .via(incomingDataFlow)
      .toMat(BroadcastHub.sink(256))(Keep.both)
      .withAttributes(ActorAttributes.logLevels(Logging.DebugLevel, Logging.DebugLevel, Logging.DebugLevel))
      .run()

  /**
    * The "final form" of the event flow.
    *
    * It consumes byte strings which are the events, and feeds them to the hub inbound.
    */
  val eventsFlow: Flow[ByteString, Nothing, NotUsed] =
    // Flow.fromSinkAndSourceCoupled(inboundSink, Source.maybe[Nothing])
    Flow.fromSinkAndSourceCoupled(inboundSink, Source.maybe[Nothing])

  /**
    * @return The source of events for the given user
    * @param userId Id of the user
    */
  def outgoingFlow(userId: Int): Source[ByteString, NotUsed] =
    broadcastOut.filter(isNotified(userId)).map{
      case (x,y) => x.render
    }

  /**
   * The "final form" of the client flow.
   *
   * Clients will connect to this server and send their id as an Identity message (e.g. "21323\n").
   *
   * The server establishes a link from the event source towards the clients, in such way that they
   * receive only the events that they are interested about.
   *
   * The incoming side of this flow extracts the client id to then properly construct the outgoing Source,
   * as it will need this identifier to notify the server which data it is interested about.
   */
  def clientFlow(): Flow[ByteString, ByteString, NotUsed] =
    val clientIdPromise = Promise[Identity]()

    // A sink that parses the client identity and completes `clientIdPromise` with it
    val incoming: Sink[ByteString, NotUsed] =
      identityParserSink.mapMaterializedValue{idFuture =>
        idFuture.onComplete{
          case Success(i) => clientIdPromise.success(i)
          case Failure(e) => clientIdPromise.failure(e)
        }
        akka.NotUsed
      }

    val outgoing = Source.futureSource(clientIdPromise.future.map { identity =>
      outgoingFlow(identity.userId)
    })

    Flow.fromSinkAndSource(incoming, outgoing)

