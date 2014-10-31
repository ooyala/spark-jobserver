package ooyala.common.akka.metrics

import com.codahale.metrics._
import java.util.concurrent.TimeUnit
import org.coursera.metrics.datadog.DatadogReporter
import org.coursera.metrics.datadog.DatadogReporter.Expansion
import org.coursera.metrics.datadog.transport.HttpTransport
import org.slf4j.LoggerFactory
import scala.util.Try

object MetricsWrapper {
  private val logger = LoggerFactory.getLogger(getClass)
  val registry: MetricRegistry = new MetricRegistry
  private var shutdownHook: Thread = null

  // Registers JVM metrics for monitoring
  JvmMetricsWrapper.registerJvmMetrics(registry)

  def startDatadogReporter(config: DatadogConfig) = {
    config.apiKey match {
      case Some(apiKey) =>
        val httpTransport = new HttpTransport.Builder().withApiKey(apiKey).build
        val datadogReporterBuilder = DatadogReporter.forRegistry(registry)

        config.hostName match {
          case Some(hostName) =>
            datadogReporterBuilder.withHost(hostName)
          case _ =>
            logger.info("No host name provided, won't report host name to datadog.")
        }

        val datadogReporter = datadogReporterBuilder
          .withTransport(httpTransport)
          .withExpansions(Expansion.ALL)
          .build


        shutdownHook = new Thread {
          override def run {
            datadogReporter.stop
          }
        }

        // Start the reporter and set up shutdown hooks
        datadogReporter.start(config.durationInSeconds, TimeUnit.SECONDS)
        Runtime.getRuntime.addShutdownHook(shutdownHook)

        logger.info("Datadog reporting started.")
      case _ =>
        logger.info("No api key provided, Datadog reporting not started.")
    }
  }

  def newGauge[T](klass: Class[_], name: String, metric: => T): Gauge[T] = {
    val metricName = MetricRegistry.name(klass, name)
    val gauge = Try(registry.register(metricName, new Gauge[T] {
      override def getValue(): T = metric
    })).map { g => g } recover { case _ =>
      registry.getGauges.get(metricName)
    }
    gauge.get.asInstanceOf[Gauge[T]]
  }

  def newCounter(klass: Class[_], name: String): Counter =
    registry.counter(MetricRegistry.name(klass, name))

  def newTimer(klass: Class[_], name: String): Timer =
    registry.timer(MetricRegistry.name(klass, name))

  def newHistogram(klass: Class[_], name: String): Histogram =
    registry.histogram(MetricRegistry.name(klass, name))

  def newMeter(klass: Class[_], name: String): Meter =
    registry.meter(MetricRegistry.name(klass, name))

  def getRegistry: MetricRegistry = {
    return registry
  }

  def shutdown = {
    if (shutdownHook != null) {
      Runtime.getRuntime.removeShutdownHook(shutdownHook)
      shutdownHook.run
    }
  }
}