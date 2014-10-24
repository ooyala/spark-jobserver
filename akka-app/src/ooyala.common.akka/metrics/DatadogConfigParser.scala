package ooyala.common.akka.metrics

import com.typesafe.config.{ConfigException, Config}
import java.net.InetAddress
import org.ini4j.ConfigParser
import org.slf4j.LoggerFactory
import scala.util.{Try, Success, Failure}

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
  // The default Datadog agent configuration file
  private val datadogAgentDefaultConfigFile = "/etc/dd-agent/datadog.conf"
  private val datadogAgentConfigMainSection = "main"

  /**
   * Parses a configuration for Datadog reporting
   *
   * Parses the reporting duration, host name, and api key from the Datadog configuration section.
   * If any of last two is missing in the configuration section, parses the missing one from the
   * datadog agent configuration file. If the host name is still not set, sets it to the local host
   * name.
   *
   * @param config a configuraiton that contains a Datadog configuration section
   * @return a configuration for Datadog reporting
   */
  def parse(config: Config): DatadogConfig = {
    // Parses the host name and the api key.
    var hostName = Try(Option(config.getString(datadogConfigPath + ".hostname"))).getOrElse(None)
    var apiKey = Try(Option(config.getString(datadogConfigPath + ".apikey"))).getOrElse(None)

    if (hostName.isEmpty || apiKey.isEmpty) {
      val datadogAgentConfigFile = Try(config.getString(datadogConfigPath + ".agentconf"))
        .getOrElse(datadogAgentDefaultConfigFile)
      val configParser = new ConfigParser
      try {
        configParser.read(datadogAgentConfigFile)
        // Parses the missing host name or api key from the datadog agent configuration file
        if (hostName.isEmpty) {
          hostName = Try(Option(configParser.get(datadogAgentConfigMainSection, "hostname")))
            .getOrElse(None)
        }
        if (apiKey.isEmpty) {
          apiKey = Try(Option(configParser.get(datadogAgentConfigMainSection, "api_key")))
            .getOrElse(None)
        }
      } catch {
        case e: Exception =>
          logger.error("Error parsing datadog agent config file " + datadogAgentConfigFile, e)
      }

      if (hostName.isEmpty) {
        // Uses local host name if the host name is still not set.
        hostName = Try(Option(InetAddress.getLocalHost.getCanonicalHostName)).getOrElse(None)
      }
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