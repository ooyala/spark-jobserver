package spark.jobserver

import akka.actor.ActorRef
import com.codahale.metrics.Meter
import ooyala.common.akka.metrics.MetricsWrapper
import ooyala.common.akka.InstrumentedActor
import scala.collection.mutable
import scala.util.Try
import spark.jobserver.io.{ JobInfo, JobDAO }

object JobStatusActor {
  case class JobInit(jobInfo: JobInfo)
  case class GetRunningJobStatus()
}

/**
 * It is an actor to manage job status updates
 *
 */
class JobStatusActor(jobDao: JobDAO) extends InstrumentedActor {
  import CommonMessages._
  import JobStatusActor._
  import spark.jobserver.util.DateUtils.dateTimeToScalaWrapper

  // jobId to its JobInfo
  private val infos = new mutable.HashMap[String, JobInfo]
  // subscribers
  private val subscribers = new mutable.HashMap[String, mutable.MultiMap[Class[_], ActorRef]]

  // metrics
  val metricNumSubscriptions = MetricsWrapper.newGauge(getClass, "num-subscriptions", subscribers.size)
  val metricNumJobInfos = MetricsWrapper.newGauge(getClass, "num-running-jobs", infos.size)
  val metricStatusRates = mutable.HashMap.empty[String, Meter]

  // timer for job latency
  private val jobLatencyTimer = MetricsWrapper.newTimer(getClass, "job-latency");
  // timer context to measure the job latency (jobId to Timer.Context mapping)
  private val latencyTimerContextMap = new mutable.HashMap[String, com.codahale.metrics.Timer.Context]

  override def wrappedReceive: Receive = {
    case GetRunningJobStatus =>
      sender ! infos.values.toSeq.sortBy(_.startTime) // TODO(kelvinchu): Use toVector instead in Scala 2.10

    case Unsubscribe(jobId, receiver) =>
      subscribers.get(jobId) match {
        case Some(jobSubscribers) =>
          jobSubscribers.transform { case (event, receivers) => receivers -= receiver }
            .retain { case (event, receivers) => receivers.nonEmpty }
          if (jobSubscribers.isEmpty) subscribers.remove(jobId)
        case None =>
          // TODO: The message below is named poorly. There may be such a job id, there are just no
          // registered subscribers for this job id.
          logger.error("No such job id " + jobId)
          sender ! NoSuchJobId
      }

    case Subscribe(jobId, receiver, events) =>
      // Subscription is independent of job life cycles. So, don't need to check infos.
      val jobSubscribers = subscribers.getOrElseUpdate(jobId, newMultiMap())
      events.foreach { event => jobSubscribers.addBinding(event, receiver) }

    case JobInit(jobInfo) =>
      // TODO (kelvinchu): Check if the jobId exists in the persistence store already
      if (!infos.contains(jobInfo.jobId)) {
        infos(jobInfo.jobId) = jobInfo
      } else {
        sender ! JobInitAlready
      }

    case msg: JobStarted =>
      latencyTimerContextMap(msg.jobId) = jobLatencyTimer.time();
      processStatus(msg, "started") {
        case (info, msg) =>
          info.copy(startTime = msg.startTime)
      }

    case msg: JobFinished =>
      stopTimer(msg.jobId)
      processStatus(msg, "finished OK", remove = true) {
        case (info, msg) =>
          info.copy(endTime = Some(msg.endTime))
      }

    case msg: JobValidationFailed =>
      processStatus(msg, "validation failed", remove = true) {
        case (info, msg) =>
          info.copy(endTime = Some(msg.endTime), error = Some(msg.err))
      }

    case msg: JobErroredOut =>
      stopTimer(msg.jobId)
      processStatus(msg, "finished with an error", remove = true) {
        case (info, msg) =>
          info.copy(endTime = Some(msg.endTime), error = Some(msg.err))
      }
  }

  private def stopTimer(jobId: String) {
    latencyTimerContextMap.get(jobId).foreach { timerContext =>
      timerContext.stop()
      latencyTimerContextMap.remove(jobId)
    }
  }

  private def processStatus[M <: StatusMessage](msg: M, logMessage: String, remove: Boolean = false)
                                               (infoModifier: (JobInfo, M) => JobInfo) {
    if (infos.contains(msg.jobId)) {
      infos(msg.jobId) = infoModifier(infos(msg.jobId), msg)
      logger.info("Job {} {}", msg.jobId: Any, logMessage)
      jobDao.saveJobInfo(infos(msg.jobId))
      publishMessage(msg.jobId, msg)
      updateMessageRate(msg)
      if (remove) infos.remove(msg.jobId)
    } else {
      logger.error("No such job id " + msg.jobId)
      sender ! NoSuchJobId
    }
  }

  private def updateMessageRate(msg: StatusMessage) {
    val msgClass = msg.getClass.getCanonicalName

    lazy val getShortName = Try(msgClass.split('.').last).toOption.getOrElse(msgClass)

    metricStatusRates.getOrElseUpdate(msgClass, MetricsWrapper.newMeter(getClass, getShortName + ".messages")).mark()
  }

  private def publishMessage(jobId: String, message: StatusMessage) {
    for (
      jobSubscribers <- subscribers.get(jobId);
      receivers <- jobSubscribers.get(message.getClass);
      receiver <- receivers
    ) {
      receiver ! message
    }
  }

  private def newMultiMap(): mutable.MultiMap[Class[_], ActorRef] =
    new mutable.HashMap[Class[_], mutable.Set[ActorRef]] with mutable.MultiMap[Class[_], ActorRef]
}
