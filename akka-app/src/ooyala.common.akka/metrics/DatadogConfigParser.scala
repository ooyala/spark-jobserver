package ooyala.common.akka.metrics

import com.typesafe.config.Config
import java.net.InetAddress
import org.slf4j.LoggerFactory
import scala.util.{Try, Success}

/**
 * Datadog configuration
 *
 * @constructor create a new configuration for datadog reporting
 * @param hostName host name used for Datadog reporting
 * @param apiKey api key used for datadog reporting
 * @param durationInSeconds durition in seconds between two Datadog reports
 */
case class DatadogConfig(hostName: Option[String],
                         apiKey: Option[String],
                         durationInSeconds: Long = 30L)

/**
 * Configuration parser for Datadog reporting
 */
object DatadogConfigParser {
  private val logger = LoggerFactory.getLogger(getClass)
  // The Datadog configuraiton path inside a job server configuration file
  private val datadogConfigPath = "spark.jobserver.metrics.datadog"

  /**
   * Parses a configuration for Datadog reporting
   *
   * Parses the reporting duration, host name, and api key from the Datadog configuration section.
   * If the host name is not set, sets it to the local host name.
   *
   *  Example config setting in spark.jobserver.metrics.datadog
   *  spark.jobserver.metrics.datadog {
   *    hostname = example
   *    apikey = example
   *    duration = 100
   *  }
   *
   * @param config a configuraiton that contains a Datadog configuration section
   * @return a configuration for Datadog reporting
   */
  def parse(config: Config): DatadogConfig = {
    // Parses the host name and the api key.
    var hostName = Try(Option(config.getString(datadogConfigPath + ".hostname"))).getOrElse(None)
    var apiKey = Try(Option(config.getString(datadogConfigPath + ".apikey"))).getOrElse(None)

    if (hostName.isEmpty) {
      // Uses local host name if the host name is not set.
      hostName = Try(Option(InetAddress.getLocalHost.getCanonicalHostName)).getOrElse(None)
    }

    // Parses the Datadog reporting duration
    Try(config.getLong(datadogConfigPath + ".duration")) match {
      case Success(duration) =>
        DatadogConfig(hostName, apiKey, duration)
      case _ =>
        DatadogConfig(hostName, apiKey)
    }
  }
}