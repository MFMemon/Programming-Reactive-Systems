package streaming

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import scala.concurrent.Future

/**
 * Implements the streaming elements that the test-suite will verify
 */
object SimpleStreaming extends ExtraStreamOps with SimpleStreamingInterface:

  /** Changes each of the streamed elements to their String values */
  def mapToStrings(ints: Source[Int, NotUsed]): Source[String, NotUsed] =
    ints.map(i => i.toString)

  /** Filters elements which are even (use the modulo operator: `%`) */
  def filterEvenValues: Flow[Int, Int, NotUsed] =
    Flow[Int].filter(i => i % 2 == 0)

  /**
   * Implements a new source by composing the previous two flow and source. 
   */
  def filterUsingPreviousFilterFlowAndMapToStrings(ints: Source[Int, NotUsed]): Source[String, NotUsed] =
    mapToStrings(ints.via(filterEvenValues))

  /**
   * Implements a new source by composing the previous two flow and source using via operator
   */
  def filterUsingPreviousFlowAndMapToStringsUsingTwoVias(ints: Source[Int, NotUsed], toString: Flow[Int, String, _]): Source[String, NotUsed] =
    ints.via(filterEvenValues).via(toString)

  /**
   * Takes the first element from the source only. The stream should be then completed once the first element has arrived.
   */
  def firstElementSource(ints: Source[Int, NotUsed]): Source[Int, NotUsed] =
    ints.take(1)

  /**
   * Runs the stream using the given materializer.
   */
  def firstElementFuture(ints: Source[Int, NotUsed])(using Materializer): Future[Int] =
    ints.runWith(Sink.head)

  // --- failure handling ---

  /**
   * Recovers [[IllegalStateException]] values to a -1 value
   */
  def recoverSingleElement(ints: Source[Int, NotUsed]): Source[Int, NotUsed] =
    ints.recover{
      case e: IllegalStateException => -1
    }

  /**
   * Recovers [[IllegalStateException]] values to the provided fallback Source
   *
   */
  def recoverToAlternateSource(ints: Source[Int, NotUsed], fallback: Source[Int, NotUsed]): Source[Int, NotUsed] =
    ints.recoverWithRetries(1, {
      case e: IllegalStateException => fallback
      }
    )

  // working with rate

  /**
   * Provides a Flow that will be able to continue receiving elements from its upstream Source
   * and "sum up" values while the downstream (the Sink to which this Flow will be attached).
   */
  def sumUntilBackpressureGoesAway: Flow[Int, Int, _] =
    Flow[Int].conflate((a,b) => a+b)

  /**
   * Provide a Flow that is able to extrapolate "invent" values by repeating the previously emitted value.
   */
  def keepRepeatingLastObservedValue: Flow[Int, Int, _] =
    Flow[Int].expand((o) => Iterator.continually(o))


