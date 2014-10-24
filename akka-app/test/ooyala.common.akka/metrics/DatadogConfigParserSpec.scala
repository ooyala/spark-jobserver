package ooyala.common.akka.metrics

import com.typesafe.config.ConfigFactory
import java.io.{BufferedWriter, File, FileWriter}
import java.net.InetAddress
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class DatadogConfigParserSpec extends FunSpec with ShouldMatchers {
  private val datadogAgentFile = File.createTempFile("jobserver", "datadog.conf")
  datadogAgentFile.deleteOnExit

  // Writes optional host name and api key into datadog agent config file
  private def writeDatadogAgentConfig(hostName: Option[String], apiKey: Option[String]) = {
    val writer = new BufferedWriter(new FileWriter(datadogAgentFile))
    writer.write("[Main]\n")
    if (hostName.isDefined) {
      writer.write("hostname: %s\n".format(hostName.get))
    }
    if (apiKey.isDefined) {
      writer.write("api_key: %s\n".format(apiKey.get))
    }
    writer.close
  }

  describe("parses Datadog config") {
    it("should parse a config in spark.jobserver.metics.datadog") {
      // Writes random hostname and api key into datadog agent config file
      writeDatadogAgentConfig(Some("random"), Some("random"))
      val configStr =
        """
        spark.jobserver.metrics.datadog {
          hostname = "test"
          apikey = "12345"
          duration = 10
        }
        """
      val config = ConfigFactory.parseString(configStr)
      val datadogConfig = DatadogConfigParser.parse(config)

      val hostName = datadogConfig.hostName
      hostName.isDefined should equal(true)
      hostName.get should equal("test")

      val apiKey = datadogConfig.apiKey
      apiKey.isDefined should equal(true)
      apiKey.get should equal("12345")

      datadogConfig.durationInSeconds should equal(10L)
    }

    it("should parse hostname from datadog agent config") {
      // Writes hostname and random api key into datadog agent config file
      writeDatadogAgentConfig(Some("test"), Some("random"))
      // Omits host name
      val configStr =
        """
        spark.jobserver.metrics.datadog {
          apikey = "12345"
          duration = 10
          agentconf = %s
        }
        """.format(datadogAgentFile.getCanonicalPath)

      val config = ConfigFactory.parseString(configStr)
      val datadogConfig = DatadogConfigParser.parse(config)

      val hostName = datadogConfig.hostName
      hostName.isDefined should equal(true)
      hostName.get should equal("test")

      val apiKey = datadogConfig.apiKey
      apiKey.isDefined should equal(true)
      // api key should be from spark.jobserver.metrics.datadog
      apiKey.get should equal("12345")

      datadogConfig.durationInSeconds should equal(10L)
    }

    it("should parse api key from datadog agent config") {
      // Writes api key and random host name into datadog agent config file
      writeDatadogAgentConfig(Some("random"), Some("12345"))
      val configStr =
        """
        spark.jobserver.metrics.datadog {
          hostname = "test"
          duration = 10
          agentconf = %s
        }
        """.format(datadogAgentFile.getCanonicalPath)

      val config = ConfigFactory.parseString(configStr)
      val datadogConfig = DatadogConfigParser.parse(config)

      val hostName = datadogConfig.hostName
      hostName.isDefined should equal(true)
      // host name should be from spark.jobserver.metrics.datadog
      hostName.get should equal("test")

      val apiKey = datadogConfig.apiKey
      apiKey.isDefined should equal(true)
      apiKey.get should equal("12345")

      datadogConfig.durationInSeconds should equal(10L)
    }

    it("should return local host name and an api key") {
      // Writes only api key in datadog agent config file
      writeDatadogAgentConfig(None, Some("12345"))
      // Omits host name and api key in spark.jobserver.metrics.datadog
      val configStr =
        """
        spark.jobserver.metrics.datadog {
          duration = 10
          agentconf = %s
        }
        """.format(datadogAgentFile.getCanonicalPath)

      val config = ConfigFactory.parseString(configStr)
      val datadogConfig = DatadogConfigParser.parse(config)

      val hostName = datadogConfig.hostName
      hostName.isDefined should equal(true)
      // When host name isn't defined in both datadog agent config file and
      // spark.jobserver.metrics.datadog, local host name should be returned
      hostName.get should equal(InetAddress.getLocalHost.getCanonicalHostName)

      val apiKey = datadogConfig.apiKey
      apiKey.isDefined should equal(true)
      apiKey.get should equal("12345")

      datadogConfig.durationInSeconds should equal(10L)
    }

    it ("should return only local host name when giving an empty datadog agent config file") {
      // Writes an empty datadog agent config file
      writeDatadogAgentConfig(None, None)
      // Omits host name and api key in spark.jobserver.metrics.datadog
      val configStr =
        """
        spark.jobserver.metrics.datadog {
          agentconf = %s
        }
        """.format(datadogAgentFile.getCanonicalPath)

      val config = ConfigFactory.parseString(configStr)
      val datadogConfig = DatadogConfigParser.parse(config)

      val hostName = datadogConfig.hostName
      hostName.isDefined should equal(true)
      hostName.get should equal(InetAddress.getLocalHost.getCanonicalHostName)

      datadogConfig.apiKey.isDefined should equal(false)
    }

    it ("should return only local host name when no datadog agent config file is given") {
      // Writes an empty datadog agent config file
      writeDatadogAgentConfig(None, None)
      // Omits host name and api key in spark.jobserver.metrics.datadog
      val configStr =
        """
        spark.jobserver.metrics.datadog {
        }
        """

      val config = ConfigFactory.parseString(configStr)
      val datadogConfig = DatadogConfigParser.parse(config)

      val hostName = datadogConfig.hostName
      hostName.isDefined should equal(true)
      hostName.get should equal(InetAddress.getLocalHost.getCanonicalHostName)

      datadogConfig.apiKey.isDefined should equal(false)
    }
  }
}