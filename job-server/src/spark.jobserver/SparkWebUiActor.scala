package spark.jobserver

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import ooyala.common.akka.InstrumentedActor
import scala.util.{Success, Failure}
import scala.concurrent.Future
import spark.jobserver.SparkWebUiActor.{SparkWorkersErrorInfo, SparkWorkersInfo, GetWorkerStatus}
import spray.can.Http
import spray.client.pipelining.{Get, sendReceive, SendReceive}
import spray.http.{HttpResponse, HttpRequest}

object SparkWebUiActor {
  // Requests
  case class GetWorkerStatus()

  // Responses
  case class SparkWorkersInfo(alive: Int, dead: Int)
  case class SparkWorkersErrorInfo(message :String)
}
/**
 * This actor pulls Spark worker status info (ALIVE, DEAD etc) from Spark admin web ui
 * Collecting worker info from HTML page is not ideal.
 * But at this time Spark does not provide public API yet to expose worker status.
 * Also, the current implementation only works for Spark standalone mode
 */
class SparkWebUiActor extends InstrumentedActor {
  import actorSystem.dispatcher // execution context for futures
  import scala.concurrent.duration._

  implicit val actorSystem: ActorSystem = context.system

  val config = context.system.settings.config

  val sparkWebHostUrls: Array[String] = getSparkHostName()
  val sparkWebHostPort = config.getInt("spark.webUrlPort")

  // implicit timeout value for ? of IO(Http)
  implicit val shortTimeout = Timeout(3 seconds)
  // get a pipeline every time we need it since pipeline could time out or die.
  // The connector will be re-used if it exists so the cost is low:
  // from http://spray.io/documentation/1.1-M8/spray-can/http-client/host-level/
  // If there is no connector actor running for the given combination of hostname,
  // port and settings spray-can will start a new one,
  // otherwise the existing one is going to be re-used.
  def pipelines: Array[Future[SendReceive]] = sparkWebHostUrls.map(url =>
    for (
      Http.HostConnectorInfo(connector, _) <- IO(Http) ? Http.HostConnectorSetup(url, port = sparkWebHostPort)
    ) yield sendReceive(connector)
  )

  override def postStop() {
    logger.info("Shutting down actor system for SparkWebUiActor")
  }

  override def wrappedReceive: Receive = {
    case GetWorkerStatus() =>
      val request = Get("/")

      val theSender = sender

      pipelines.map {pipeline =>
        val responseFuture: Future[HttpResponse] = pipeline.flatMap(_(request))
        responseFuture onComplete {
          case Success(httpResponse) =>
            val content = httpResponse.entity.asString.replace('\n', ' ');

            // regex to detect the #running tasks
            val runningTaskRegex = """.*<li><strong>Applications:</strong>\s*(\d+)\s+Running.*""".r
            content match {
              case runningTaskRegex(runningTaskStr) =>
                if (runningTaskStr != null && runningTaskStr.length > 0 && runningTaskStr.toInt > 0) {
                  // we believe it is a active master if it has active running tasks
                  // we only check the workers on active master
                  val aliveWorkerNum = "<td>ALIVE</td>".r.findAllIn(content).length
                  val deadWorkerNum = "<td>DEAD</td>".r.findAllIn(content).length

                  theSender ! SparkWorkersInfo(aliveWorkerNum, deadWorkerNum)
                }
              case _ => throw new RuntimeException("Could not parse HTML response: '" + content + "'")
            }
          case Failure(error) =>
            val msg = "Failed to retrieve Spark web UI: " +  error.getMessage
            logger.error(msg)
        }
      }
  }

  def getSparkHostName(): Array[String] = {
    val master = config.getString("spark.master")
    // Regular expression used for local[N] and local[*] master formats
    val LOCAL_N_REGEX = """local\[([0-9\*]+)\]""".r
    // Regular expression for connecting to Spark deploy clusters
    val SPARK_REGEX = """spark://(.*)""".r

    master match {
      case "localhost" | "local" | LOCAL_N_REGEX(_) => Array("localhost")
      case SPARK_REGEX(sparkUrl) =>
        val masterUrls = sparkUrl.split(",").map(s => {
          val splits = s.split(":")
          splits(0)
        })
        masterUrls
      case _ => throw new RuntimeException("Could not parse Master URL: '" + master + "'")
    }
  }


}
