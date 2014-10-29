package ooyala.common.akka.metrics

import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

class DatadogConfigParserSpec extends FunSpec with ShouldMatchers {

  describe("parses Datadog config") {
    it("should parse a valid config") {
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

    it("should return local host name and an api key") {
      // Omits host name
      val configStr =
        """
        spark.jobserver.metrics.datadog {
          apikey = "12345"
          duration = 10
        }
        """

      val config = ConfigFactory.parseString(configStr)
      val datadogConfig = DatadogConfigParser.parse(config)

      val hostName = datadogConfig.hostName
      hostName.isDefined should equal(true)
      // When host name isn't defined in config file, local host name should be returned
      hostName.get should equal(InetAddress.getLocalHost.getCanonicalHostName)

      val apiKey = datadogConfig.apiKey
      apiKey.isDefined should equal(true)
      apiKey.get should equal("12345")

      datadogConfig.durationInSeconds should equal(10L)
    }

    it ("should return only local host name") {
      // Omits host name and api key
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