package spark.jobserver

import akka.actor.ActorRef
import ooyala.common.akka.InstrumentedActor
import ooyala.common.akka.metrics.MetricsWrapper
import spark.jobserver.util.GuavaCacheUtils._

import scala.collection.mutable

/**
 * It is an actor to manage results that are returned from jobs.
 *
 * TODO: support multiple subscribers for same JobID
 */
class JobResultActor extends InstrumentedActor {
  import CommonMessages._

  private val config = context.system.settings.config

  private val cache = buildCache[String, Any](config).withMetrics(getClass)

  private val subscribers = mutable.HashMap.empty[String, ActorRef] // subscribers

  // metrics
  private val metricSubscribers =
    MetricsWrapper.newGauge(getClass, "subscribers-size", subscribers.size)

  def wrappedReceive: Receive = {
    case Subscribe(jobId, receiver, events) =>
      if (events.contains(classOf[JobResult])) {
        subscribers(jobId) = receiver
        logger.info("Added receiver {} to subscriber list for JobID {}", receiver, jobId: Any)
      }

    case Unsubscribe(jobId, receiver) =>
      if (!subscribers.contains(jobId)) {
        sender ! NoSuchJobId
      } else {
        subscribers.remove(jobId)
        logger.info("Removed subscriber list for JobID {}", jobId)
      }

    case GetJobResult(jobId) =>
      sender ! Option(cache.getIfPresent(jobId)).map(JobResult(jobId, _)).getOrElse(NoSuchJobId)

    case JobResult(jobId, result) =>
      cache.put(jobId, result)
      logger.debug("Received job results for JobID {}", jobId)
      subscribers.get(jobId).foreach(_ ! JobResult(jobId, result))
      subscribers.remove(jobId)
  }
}